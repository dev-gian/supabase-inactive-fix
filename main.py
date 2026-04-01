# main.py

import json
import os
import logging
from helpers.utils import generate_secure_random_string
from services.supabase_service import SupabaseClient

log_failed_databases = True
detailed_status_report = True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def mask_key(key):
    if not key:
        return "None"
    return f"{key[:6]}...{key[-4:]}" if len(key) > 10 else "***"

def main():
    try:
        with open('config.json', 'r') as config_file:
            configs = json.load(config_file)
        logging.info(f"Loaded {len(configs)} database configs")
    except FileNotFoundError:
        logging.error("Configuration file 'config.json' not found.")
        return
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing 'config.json': {e}")
        return

    all_successful = True
    failed_databases = [] if log_failed_databases else None
    status_report = [] if detailed_status_report else None

    for config in configs:
        name = config.get('name', 'Unnamed Database')
        url = config.get('supabase_url')
        key = config.get('supabase_key')
        table_name = config.get('table_name', 'KeepAlive')
        key_env_var = config.get('supabase_key_env')

        if key_env_var:
            key = os.getenv(key_env_var)
            logging.info(f"[{name}] Using env var: {key_env_var} → exists={bool(key)}")

        logging.info(f"[{name}] URL: {url}")
        logging.info(f"[{name}] KEY present: {bool(key)} | len={len(key) if key else 0} | preview={mask_key(key)}")
        logging.info(f"[{name}] TABLE: {table_name}")

        if not url or not key:
            logging.error(f"[{name}] Missing URL or KEY. Skipping.")
            all_successful = False
            if log_failed_databases:
                failed_databases.append(name)
            continue

        logging.info(f"Processing database: {name}")

        supabase_client = SupabaseClient(url, key, table_name)

        random_name = generate_secure_random_string(10)
        logging.info(f"[{name}] Generated value: {random_name}")

        logging.info(f"[{name}] Attempting INSERT into '{table_name}' with payload: {{'name': '{random_name}'}}")
        success_insert = supabase_client.insert_random_name(random_name)

        if not success_insert:
            logging.error(f"[{name}] INSERT failed")
            all_successful = False
            if log_failed_databases:
                failed_databases.append(name)
            continue

        logging.info(f"[{name}] INSERT success")

        logging.info(f"[{name}] Fetching row count...")
        count = supabase_client.get_table_count()

        if count is None:
            logging.error(f"[{name}] COUNT failed for table '{table_name}'")
            all_successful = False
            if log_failed_databases:
                failed_databases.append(name)
            continue

        logging.info(f"[{name}] Row count: {count}")

        success_delete = None

        if count > 10:
            logging.info(f"[{name}] Count > 10 → deleting random row")
            success_delete = supabase_client.delete_random_entry()
            if not success_delete:
                logging.error(f"[{name}] DELETE failed")
                all_successful = False
                if log_failed_databases and name not in failed_databases:
                    failed_databases.append(name)
            else:
                logging.info(f"[{name}] DELETE success")
        else:
            logging.info(f"[{name}] Count ≤ 10 → no deletion")

        if detailed_status_report:
            status_report.append({
                'name': name,
                'success_insert': success_insert,
                'success_delete': success_delete,
                'count': count
            })

    if all_successful:
        logging.info("All database actions were successful.")
    else:
        logging.warning("Some database actions failed.")

        if log_failed_databases and failed_databases:
            logging.warning("Failed databases:")
            for db_name in failed_databases:
                logging.warning(f"- {db_name}")

    if detailed_status_report and status_report:
        logging.info("\nDetailed Status Report:")
        for status in status_report:
            logging.info(f"Database: {status['name']}")
            logging.info(f"  Insert Success: {status['success_insert']}")
            logging.info(f"  Entry Count: {status['count']}")
            logging.info(f"  Delete Success: {status['success_delete'] if status['success_delete'] is not None else 'N/A'}")


if __name__ == "__main__":
    main()
