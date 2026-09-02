# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: MLB Stats API Ingestion
# MAGIC Pulls daily schedule + boxscores from the MLB Stats API and lands raw JSON
# MAGIC into a Bronze Delta table. No cleaning/flattening happens here on purpose —
# MAGIC Bronze should always reflect exactly what the API returned.

# COMMAND ----------

import requests
import json
from datetime import date, timedelta
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config
# MAGIC This notebook runs in two modes, controlled by job parameters:
# MAGIC
# MAGIC - **Backfill mode**: pass `start_date` and `end_date` widget values
# MAGIC   (e.g. "2026-03-25" through today) to pull a historical range once.
# MAGIC - **Daily mode**: leave both widgets blank (the default). The
# MAGIC   notebook then automatically computes **yesterday** and pulls just
# MAGIC   that one day — this is what the daily schedule trigger will run
# MAGIC   unattended, with no parameters needed.
# MAGIC
# MAGIC Using yesterday (not today) for daily mode matters: if this runs
# MAGIC early morning, today's games haven't been played yet, and even
# MAGIC late-night West Coast games from *last* night may not be finalized
# MAGIC the instant midnight passes. Yesterday's games are reliably final.

# COMMAND ----------

dbutils.widgets.text("start_date", "", "Backfill start date (YYYY-MM-DD, optional)")
dbutils.widgets.text("end_date", "", "Backfill end date (YYYY-MM-DD, optional)")

start_param = dbutils.widgets.get("start_date").strip()
end_param = dbutils.widgets.get("end_date").strip()

if start_param and end_param:
    START_DATE = date.fromisoformat(start_param)
    END_DATE = date.fromisoformat(end_param)
    print(f"[BACKFILL MODE] {START_DATE} through {END_DATE}")
else:
    # Daily mode: no params passed, default to yesterday only
    START_DATE = date.today() - timedelta(days=1)
    END_DATE = START_DATE
    print(f"[DAILY MODE] Pulling {START_DATE}")

BASE_URL = "https://statsapi.mlb.com/api/v1"
BRONZE_SCHEDULE_TABLE = "bronze.mlb_schedule_raw"
BRONZE_BOXSCORE_TABLE = "bronze.mlb_boxscore_raw"
BRONZE_TEAMS_TABLE = "bronze.mlb_teams_raw"

# COMMAND ----------

def daterange(start: date, end: date):
    for n in range((end - start).days + 1):
        yield start + timedelta(days=n)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Determine which dates actually need fetching
# MAGIC A naive "have I fetched this date before?" check is wrong: a date
# MAGIC pulled while games were still `Scheduled` or `In Progress` (e.g.
# MAGIC during backfill, if that date was "today" at the time) would get
# MAGIC marked done forever, even though the real games happened later and
# MAGIC never got re-pulled. Instead, a date only counts as "done" once
# MAGIC every game on it has reached a **terminal status** — one that won't
# MAGIC change anymore. Anything else gets re-fetched.

# COMMAND ----------

TERMINAL_STATUSES = {"Final", "Completed Early", "Postponed", "Cancelled", "Suspended"}
# Postponed/Cancelled are terminal for THIS game_pk specifically — a
# makeup game gets its own separate game_pk on a new date, so we don't
# need to keep re-checking the original postponed entry.

def date_needs_refetch(game_date: date) -> bool:
    """True if this date was never fetched, or was fetched but still has
    a game in a non-terminal status (meaning something could still change)."""
    try:
        rows = spark.table(BRONZE_SCHEDULE_TABLE) \
            .filter(f"source_date = '{game_date.isoformat()}'") \
            .orderBy(col("ingestion_timestamp").desc()) \
            .select("raw_json").limit(1).collect()
    except Exception:
        return True  # table doesn't exist yet — first run

    if not rows:
        return True  # never fetched

    parsed = json.loads(rows[0].raw_json)
    for day in parsed.get("dates", []):
        for game in day.get("games", []):
            status = game.get("status", {}).get("detailedState")
            if status not in TERMINAL_STATUSES:
                return True  # still pending — needs a fresh pull
    return False

# COMMAND ----------

dates_to_fetch = [d for d in daterange(START_DATE, END_DATE) if date_needs_refetch(d)]
dates_skipped = (END_DATE - START_DATE).days + 1 - len(dates_to_fetch)

print(f"Dates needing fetch: {len(dates_to_fetch)}")
print(f"Dates skipped (already fully terminal): {dates_skipped}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Pull schedule for those dates
# MAGIC The schedule endpoint tells us which games happened (or were postponed)
# MAGIC on a given date, and gives us the `gamePk` needed to pull boxscores.

# COMMAND ----------

def fetch_schedule(game_date: date) -> dict:
    """Fetch the MLB schedule for a single date. Returns raw JSON."""
    url = f"{BASE_URL}/schedule"
    params = {
        "sportId": 1,          # 1 = MLB
        "date": game_date.strftime("%m/%d/%Y"),
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

# COMMAND ----------

schedule_rows = []

for d in dates_to_fetch:
    try:
        raw = fetch_schedule(d)
    except requests.RequestException as e:
        # Real pipelines don't die on one bad call — log and continue
        print(f"[WARN] schedule fetch failed for {d}: {e}")
        continue

    schedule_rows.append(
        Row(
            source_date=d.isoformat(),
            raw_json=json.dumps(raw),
        )
    )

print(f"Fetched schedule for {len(schedule_rows)} dates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write schedule to Bronze
# MAGIC Landing as raw JSON strings + ingestion timestamp. Silver's dedup
# MAGIC step already keeps the latest ingestion per date, so appending a
# MAGIC fresher pull for a previously non-terminal date is safe — Silver
# MAGIC will pick it up as the newer version automatically.

# COMMAND ----------

if schedule_rows:
    schedule_df = spark.createDataFrame(schedule_rows) \
        .withColumn("ingestion_timestamp", current_timestamp())

    schedule_df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(BRONZE_SCHEDULE_TABLE)

    display(schedule_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Extract gamePks from the dates we just (re)fetched
# MAGIC Deliberately only from `schedule_rows` (this run's fetches), not
# MAGIC from the full historical schedule table — we only need boxscores
# MAGIC for games on dates whose status could have changed. Dates that were
# MAGIC already fully terminal were skipped above, so their boxscores don't
# MAGIC need touching either.

# COMMAND ----------

def extract_game_pks(schedule_json: dict) -> list[int]:
    """Pull gamePk values out of a schedule response, skipping games
    that have no gamePk (shouldn't happen, but defensive)."""
    pks = []
    for day in schedule_json.get("dates", []):
        for game in day.get("games", []):
            pk = game.get("gamePk")
            if pk is not None:
                pks.append(pk)
    return pks

# COMMAND ----------

all_game_pks = []
for row in schedule_rows:
    raw = json.loads(row.raw_json)
    all_game_pks.extend(extract_game_pks(raw))

all_game_pks = sorted(set(all_game_pks))  # dedupe in case of overlapping pulls
print(f"Found {len(all_game_pks)} unique games needing a boxscore pull")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Pull boxscores for each game
# MAGIC This is the slow part — one API call per game. For a large backfill,
# MAGIC consider batching with a short sleep to be polite to the API, or
# MAGIC parallelizing carefully (watch for rate limiting).

# COMMAND ----------

import time

def fetch_boxscore(game_pk: int) -> dict:
    url = f"{BASE_URL}/game/{game_pk}/boxscore"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

# COMMAND ----------

boxscore_rows = []
failed_pks = []

for pk in all_game_pks:
    try:
        raw = fetch_boxscore(pk)
        boxscore_rows.append(
            Row(
                game_pk=pk,
                raw_json=json.dumps(raw),
            )
        )
    except requests.RequestException as e:
        print(f"[WARN] boxscore fetch failed for gamePk {pk}: {e}")
        failed_pks.append(pk)
    time.sleep(0.1)  # small politeness delay

print(f"Fetched {len(boxscore_rows)} boxscores, {len(failed_pks)} failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Write boxscores to Bronze
# MAGIC Same reasoning as schedule — appending fresher pulls for games that
# MAGIC previously had incomplete/pre-game stats. Silver dedups to the
# MAGIC latest ingestion per game_pk, so this is safe and self-correcting.

# COMMAND ----------

if boxscore_rows:
    boxscore_df = spark.createDataFrame(boxscore_rows) \
        .withColumn("ingestion_timestamp", current_timestamp())

    boxscore_df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(BRONZE_BOXSCORE_TABLE)

    display(boxscore_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Pull team reference data
# MAGIC Unlike schedule/boxscores, the `/teams` list barely changes — but it
# MAGIC still belongs in Bronze, not fetched live from Gold. Landing it here
# MAGIC means Gold never calls the API directly, and we have a record of
# MAGIC exactly what the API returned and when, same as everything else.

# COMMAND ----------

def fetch_teams() -> dict:
    url = f"{BASE_URL}/teams"
    resp = requests.get(url, params={"sportId": 1}, timeout=30)
    resp.raise_for_status()
    return resp.json()

# COMMAND ----------

try:
    teams_raw = fetch_teams()
    teams_row = [Row(raw_json=json.dumps(teams_raw))]

    teams_df = spark.createDataFrame(teams_row) \
        .withColumn("ingestion_timestamp", current_timestamp())

    # Overwrite is fine here — team reference data isn't a historical
    # event log the way schedule/boxscores are. We just want the latest
    # snapshot each time this runs (teams rarely change mid-season, but
    # e.g. venue names or division alignment could in theory).
    teams_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(BRONZE_TEAMS_TABLE)

    print("Teams reference data landed in Bronze.")
    display(teams_df)
except requests.RequestException as e:
    print(f"[WARN] teams fetch failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes for later (Silver layer)
# MAGIC - Some games in `failed_pks` may need retry logic — postponed/suspended
# MAGIC   games sometimes 404 or return incomplete data.
# MAGIC - Boxscore JSON is deeply nested (teams -> players -> stats). Silver
# MAGIC   will need `explode()` or Python-side flattening before writing
# MAGIC   structured Delta tables.
# MAGIC - Watch for players who appear under different `teamId`s across dates
# MAGIC   (trades) — decide in Silver whether dim_players tracks history.
