import subprocess
from dagster import asset, define_asset_job, ScheduleDefinition, Definitions


# --- 1. Scrape Telegram ---
@asset
def raw_telegram_data():
    """Scrapes data from Telegram and saves to JSON/Images."""
    # We use subprocess to run your existing scripts
    result = subprocess.run(
        ["python", "src/scraper.py"], capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"Scraper failed: {result.stderr}")
    return "Scraping Complete"


# --- 2. Load Raw Data ---
@asset(deps=[raw_telegram_data])
def raw_database_tables():
    """Loads JSON data into Postgres raw tables."""
    result = subprocess.run(["python", "src/loader.py"], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Loader failed: {result.stderr}")
    return "Loading Complete"


# --- 3. Run Object Detection (YOLO) ---
@asset(deps=[raw_database_tables])
def object_detection_results():
    """Runs YOLOv8 on images and loads to DB."""
    # Run detection
    det_res = subprocess.run(
        ["python", "src/yolo_detect.py"], capture_output=True, text=True
    )
    if det_res.returncode != 0:
        raise Exception(f"YOLO failed: {det_res.stderr}")

    # Load results to DB
    load_res = subprocess.run(
        ["python", "src/load_yolo.py"], capture_output=True, text=True
    )
    if load_res.returncode != 0:
        raise Exception(f"YOLO Loader failed: {load_res.stderr}")

    return "Object Detection & Loading Complete"


# --- 4. Run dbt Transformation ---
@asset(deps=[object_detection_results])
def dbt_models():
    """Runs dbt to build the Data Warehouse."""
    result = subprocess.run(["dbt", "run"], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")
    return "dbt Transformation Complete"


# --- Job & Schedule ---
daily_update_job = define_asset_job(name="daily_medical_update", selection="*")

# Run everyday at midnight
daily_schedule = ScheduleDefinition(
    job=daily_update_job,
    cron_schedule="0 0 * * *",
)

defs = Definitions(
    assets=[
        raw_telegram_data,
        raw_database_tables,
        object_detection_results,
        dbt_models,
    ],
    schedules=[daily_schedule],
)
