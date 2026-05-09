"""
Bronze layer: CSV to Parquet with metadata.
Adds _source_file, _batch_id, _ingestion_ts, partitions by date.
"""

import os
import uuid
import logging
from datetime import datetime
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

RAW_DIR    = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
BRONZE_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'bronze')

SOURCE_FILES = {
    'branches':     'branches.csv',
    'merchants':    'merchants.csv',
    'customers':    'customers.csv',
    'accounts':     'accounts.csv',
    'transactions': 'transactions.csv',
}


def load_table(table_name: str, filename: str, batch_id: str) -> int:
    """Load one CSV file into Bronze as Parquet. Keep everything as strings."""
    src = os.path.join(RAW_DIR, filename)

    if not os.path.exists(src):
        log.warning(f"File not found, skipping: {src}")
        return 0

    log.info(f"Loading {filename} -> bronze/{table_name}/")

    # Read raw with all columns as strings
    df = pd.read_csv(src, dtype=str, low_memory=False)

    # Add Bronze metadata
    df['_source_file']       = filename
    df['_batch_id']          = batch_id
    df['_ingestion_ts']      = datetime.utcnow().isoformat()
    df['_ingestion_date']    = datetime.utcnow().strftime('%Y-%m-%d')
    df['_source_row_number'] = range(1, len(df) + 1)

    # Partition by ingestion date so multiple runs don't overwrite
    partition_date = datetime.utcnow().strftime('%Y-%m-%d')
    out_dir = os.path.join(BRONZE_DIR, table_name, f'date={partition_date}')
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, f'{table_name}_{batch_id[:8]}.parquet')
    df.to_parquet(out_path, index=False, engine='pyarrow')

    log.info(f"  Wrote {len(df):,} rows -> {out_path}")
    return len(df)


def run():
    batch_id = str(uuid.uuid4())
    log.info("=" * 60)
    log.info("BRONZE INGESTION STARTING")
    log.info(f"Batch: {batch_id}")
    log.info("=" * 60)

    os.makedirs(BRONZE_DIR, exist_ok=True)
    total = 0

    for table, filename in SOURCE_FILES.items():
        n = load_table(table, filename, batch_id)
        total += n

    log.info("=" * 60)
    log.info(f"BRONZE DONE — {total:,} total rows across all tables")
    log.info("=" * 60)


if __name__ == '__main__':
    run()