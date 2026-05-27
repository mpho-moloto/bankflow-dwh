"""
db_manager.py
-------------
GOLD LAYER LOADER — Silver Parquet → PostgreSQL Star Schema

This is where Kimball dimensional modelling comes to life.

What happens here:
  1. Dimensions first (branches, merchants, customers, accounts)
     - SCD Type 2 for customers and accounts: if a tracked field changes,
       we expire the old row and insert a new version.
     - SCD Type 1 for merchants and branches: just upsert.
  2. Fact transactions via a staging table + SQL INSERT
     - Incremental: we only load rows newer than the max date in the fact table.
     - Staging table pattern: bulk COPY into an UNLOGGED table, then
       one big INSERT that joins to all dimensions to resolve surrogate keys.

Why staging?
  Doing dimension lookups row-by-row in Python would be slow.
  The staging approach lets the Postgres query planner do the joins
  efficiently in a single pass.

Run:
    python warehouse/db_manager.py
"""

import os
import io
import uuid
import glob
import logging
from datetime import date, datetime

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

SILVER_DIR = os.getenv('SILVER_DATA_PATH', 'data/silver')

DB_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER', 'postgres')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'postgres')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5433')}/"
    f"{os.getenv('POSTGRES_DB', 'bankflow_dwh')}"
)

# Columns we track for SCD Type 2 — if any of these change, we create a new version
SCD2_CUSTOMER_COLS = ['customer_segment', 'income_band', 'risk_rating', 'is_active', 'kyc_verified']
SCD2_ACCOUNT_COLS  = ['status', 'credit_limit', 'overdraft_limit', 'interest_rate']


class GoldLoader:

    def __init__(self):
        self.engine   = create_engine(DB_URL, pool_pre_ping=True)
        self.batch_id = str(uuid.uuid4())
        log.info(f"Batch ID: {self.batch_id}")

    # ─────────────────────────────────────────────────────────
    # BULK LOAD HELPER (PostgreSQL COPY — very fast)
    # ─────────────────────────────────────────────────────────

    def _bulk_copy(self, df: pd.DataFrame, table: str):
        """
        Uses PostgreSQL's COPY command instead of INSERT for speed.
        For 500k rows, COPY is ~10x faster than individual INSERTs.
        """
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, sep='\t', na_rep='')
        buf.seek(0)

        raw = self.engine.raw_connection()
        try:
            cur = raw.cursor()
            cur.copy_from(buf, table, sep='\t', null='')
            raw.commit()
        finally:
            cur.close()
            raw.close()

    # ─────────────────────────────────────────────────────────
    # BRANCHES — SCD Type 1 (simple upsert, no history needed)
    # ─────────────────────────────────────────────────────────

    def load_branches(self):
        log.info("Loading branches...")
        path = os.path.join(SILVER_DIR, 'branches')
        if not os.path.exists(path):
            log.warning("Silver/branches not found, skipping")
            return

        df = pd.read_parquet(path)
        df['dw_batch_id'] = self.batch_id

        sql = text("""
            INSERT INTO dim_branches (branch_id, branch_name, city, province,
                                      region, opened_date, is_active, dw_batch_id)
            VALUES (:branch_id, :branch_name, :city, :province,
                    :region, :opened_date, :is_active, :dw_batch_id)
            ON CONFLICT (branch_id)
            DO UPDATE SET
                branch_name = EXCLUDED.branch_name,
                city        = EXCLUDED.city,
                province    = EXCLUDED.province,
                region      = EXCLUDED.region,
                is_active   = EXCLUDED.is_active,
                dw_batch_id = EXCLUDED.dw_batch_id
        """)

        with self.engine.begin() as conn:
            conn.execute(sql, df[['branch_id', 'branch_name', 'city', 'province',
                                   'region', 'opened_date', 'is_active', 'dw_batch_id']]
                         .to_dict(orient='records'))
        log.info(f"  Branches loaded: {len(df):,}")

    # ─────────────────────────────────────────────────────────
    # MERCHANTS — SCD Type 1
    # ─────────────────────────────────────────────────────────

    def load_merchants(self):
        log.info("Loading merchants...")
        path = os.path.join(SILVER_DIR, 'merchants')
        if not os.path.exists(path):
            log.warning("Silver/merchants not found, skipping")
            return

        df = pd.read_parquet(path)
        df['dw_batch_id'] = self.batch_id

        sql = text("""
            INSERT INTO dim_merchants (merchant_id, merchant_name, merchant_category,
                                       mcc_code, is_online, dw_batch_id)
            VALUES (:merchant_id, :merchant_name, :merchant_category,
                    :mcc_code, :is_online, :dw_batch_id)
            ON CONFLICT (merchant_id)
            DO UPDATE SET
                merchant_name     = EXCLUDED.merchant_name,
                merchant_category = EXCLUDED.merchant_category,
                mcc_code          = EXCLUDED.mcc_code,
                is_online         = EXCLUDED.is_online,
                dw_batch_id       = EXCLUDED.dw_batch_id
        """)

        with self.engine.begin() as conn:
            conn.execute(sql, df[['merchant_id', 'merchant_name', 'merchant_category',
                                   'mcc_code', 'is_online', 'dw_batch_id']]
                         .to_dict(orient='records'))
        log.info(f"  Merchants loaded: {len(df):,}")

    # ─────────────────────────────────────────────────────────
    # CUSTOMERS — SCD TYPE 2
    # ─────────────────────────────────────────────────────────
    #
    # For each incoming customer row we:
    #   a) If the customer doesn't exist yet → INSERT (version 1)
    #   b) If they exist AND none of the tracked columns changed → do nothing
    #   c) If a tracked column changed:
    #      - UPDATE the current row: set scd_end_date = today, scd_is_current = FALSE
    #      - INSERT a new row with the new values, version + 1
    #
    # ─────────────────────────────────────────────────────────

    def load_customers_scd2(self):
        log.info("Loading customers (SCD Type 2)...")
        path = os.path.join(SILVER_DIR, 'customers')
        if not os.path.exists(path):
            log.warning("Silver/customers not found, skipping")
            return

        incoming = pd.read_parquet(path)
        today    = date.today()
        inserts  = 0
        updates  = 0

        with self.engine.begin() as conn:
            for _, row in incoming.iterrows():
                cid = row['customer_id']

                # Fetch the currently active row for this customer (if any)
                existing = conn.execute(text("""
                    SELECT customer_key, customer_segment, income_band,
                           risk_rating, is_active, kyc_verified, scd_version
                    FROM dim_customers
                    WHERE customer_id = :cid AND scd_is_current = TRUE
                    LIMIT 1
                """), {'cid': cid}).fetchone()

                if existing is None:
                    # Brand new customer — straight insert
                    conn.execute(text("""
                        INSERT INTO dim_customers
                            (customer_id, full_name, email, date_of_birth, age_band,
                             gender, province, city, customer_segment, income_band,
                             risk_rating, kyc_verified, onboarding_date, is_active,
                             scd_start_date, scd_end_date, scd_is_current, scd_version,
                             dw_batch_id)
                        VALUES
                            (:customer_id, :full_name, :email, :date_of_birth, :age_band,
                             :gender, :province, :city, :customer_segment, :income_band,
                             :risk_rating, :kyc_verified, :onboarding_date, :is_active,
                             :today, NULL, TRUE, 1, :batch_id)
                    """), {**row.to_dict(), 'today': today, 'batch_id': self.batch_id})
                    inserts += 1

                else:
                    # Check if any of our tracked SCD2 columns changed
                    changed = any([
                        str(existing.customer_segment) != str(row.get('customer_segment', '')),
                        str(existing.income_band)       != str(row.get('income_band', '')),
                        str(existing.risk_rating)       != str(row.get('risk_rating', '')),
                        bool(existing.is_active)        != bool(row.get('is_active', True)),
                        bool(existing.kyc_verified)     != bool(row.get('kyc_verified', False)),
                    ])

                    if changed:
                        # Expire the old row
                        conn.execute(text("""
                            UPDATE dim_customers
                               SET scd_end_date   = :today,
                                   scd_is_current = FALSE
                             WHERE customer_key = :key
                        """), {'today': today, 'key': existing.customer_key})

                        # Insert the new version
                        conn.execute(text("""
                            INSERT INTO dim_customers
                                (customer_id, full_name, email, date_of_birth, age_band,
                                 gender, province, city, customer_segment, income_band,
                                 risk_rating, kyc_verified, onboarding_date, is_active,
                                 scd_start_date, scd_end_date, scd_is_current, scd_version,
                                 dw_batch_id)
                            VALUES
                                (:customer_id, :full_name, :email, :date_of_birth, :age_band,
                                 :gender, :province, :city, :customer_segment, :income_band,
                                 :risk_rating, :kyc_verified, :onboarding_date, :is_active,
                                 :today, NULL, TRUE, :version, :batch_id)
                        """), {**row.to_dict(),
                               'today': today,
                               'version': existing.scd_version + 1,
                               'batch_id': self.batch_id})
                        updates += 1

        log.info(f"  Customers: {inserts:,} new, {updates:,} SCD2 updates")

    # ─────────────────────────────────────────────────────────
    # ACCOUNTS — SCD TYPE 2 (with branch_key)
    # ─────────────────────────────────────────────────────────

    def load_accounts_scd2(self):
        log.info("Loading accounts (SCD Type 2)...")
        path = os.path.join(SILVER_DIR, 'accounts')
        if not os.path.exists(path):
            log.warning("Silver/accounts not found, skipping")
            return

        incoming = pd.read_parquet(path)
        today    = date.today()
        inserts  = 0
        updates  = 0

        with self.engine.begin() as conn:
            for _, row in incoming.iterrows():
                aid = row['account_id']

                # Get branch_key from dim_branches using branch_id
                branch_result = conn.execute(text("""
                    SELECT branch_key FROM dim_branches WHERE branch_id = :branch_id LIMIT 1
                """), {'branch_id': row['branch_id']}).fetchone()
                branch_key = branch_result[0] if branch_result else None

                # Resolve customer surrogate key (always use current row)
                cust_key = conn.execute(text("""
                    SELECT customer_key FROM dim_customers
                    WHERE customer_id = :cid AND scd_is_current = TRUE
                    LIMIT 1
                """), {'cid': row['customer_id']}).scalar()

                existing = conn.execute(text("""
                    SELECT account_key, status, credit_limit,
                           overdraft_limit, interest_rate, scd_version
                    FROM dim_accounts
                    WHERE account_id = :aid AND scd_is_current = TRUE
                    LIMIT 1
                """), {'aid': aid}).fetchone()

                if existing is None:
                    conn.execute(text("""
                        INSERT INTO dim_accounts
                            (account_id, customer_key, customer_id, account_type,
                             account_number, currency, credit_limit, interest_rate,
                             overdraft_limit, status, open_date, close_date,
                             account_age_days, branch_key,
                             scd_start_date, scd_end_date, scd_is_current, scd_version,
                             dw_batch_id)
                        VALUES
                            (:account_id, :cust_key, :customer_id, :account_type,
                             :account_number, :currency, :credit_limit, :interest_rate,
                             :overdraft_limit, :status, :open_date, :close_date,
                             :account_age_days, :branch_key,
                             :today, NULL, TRUE, 1, :batch_id)
                    """), {**row.to_dict(),
                           'cust_key': cust_key,
                           'branch_key': branch_key,
                           'today': today,
                           'batch_id': self.batch_id})
                    inserts += 1

                else:
                    changed = any([
                        str(existing.status)          != str(row.get('status', '')),
                        float(existing.credit_limit   or 0) != float(row.get('credit_limit')   or 0),
                        float(existing.overdraft_limit or 0) != float(row.get('overdraft_limit') or 0),
                        float(existing.interest_rate  or 0) != float(row.get('interest_rate')  or 0),
                    ])

                    if changed:
                        conn.execute(text("""
                            UPDATE dim_accounts
                               SET scd_end_date   = :today,
                                   scd_is_current = FALSE
                             WHERE account_key = :key
                        """), {'today': today, 'key': existing.account_key})

                        conn.execute(text("""
                            INSERT INTO dim_accounts
                                (account_id, customer_key, customer_id, account_type,
                                 account_number, currency, credit_limit, interest_rate,
                                 overdraft_limit, status, open_date, close_date,
                                 account_age_days, branch_key,
                                 scd_start_date, scd_end_date, scd_is_current, scd_version,
                                 dw_batch_id)
                            VALUES
                                (:account_id, :cust_key, :customer_id, :account_type,
                                 :account_number, :currency, :credit_limit, :interest_rate,
                                 :overdraft_limit, :status, :open_date, :close_date,
                                 :account_age_days, :branch_key,
                                 :today, NULL, TRUE, :version, :batch_id)
                        """), {**row.to_dict(),
                               'cust_key': cust_key,
                               'branch_key': branch_key,
                               'today': today,
                               'version': existing.scd_version + 1,
                               'batch_id': self.batch_id})
                        updates += 1

        log.info(f"  Accounts: {inserts:,} new, {updates:,} SCD2 updates")

    # ─────────────────────────────────────────────────────────
    # FACT TRANSACTIONS — incremental load via staging table
    # ─────────────────────────────────────────────────────────

    def load_fact_transactions(self):
        log.info("Loading fact transactions (incremental)...")

        # Watermark: only load records newer than the latest date we've already loaded
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT COALESCE(MAX(transaction_date), '1900-01-01') FROM fact_transactions")
            ).scalar()
        last_date = result if isinstance(result, date) else date.fromisoformat(str(result))
        log.info(f"  Watermark: loading transactions after {last_date}")

        # Recreate the staging table fresh
        with self.engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE stg_fact_transactions"))

        # Stream Silver Parquet in chunks → staging
        target_cols = [
            'transaction_id', 'account_id', 'merchant_id', 'transaction_type',
            'transaction_date', 'transaction_time', 'transaction_hour',
            'amount', 'is_debit', 'balance_after', 'status',
            'is_fraud_flag', 'fraud_score', 'fraud_risk_band',
            'is_weekend', 'is_night_transaction', 'amount_band',
            'daily_txn_count', 'daily_spend_total', 'reference',
            'description', 'channel',
        ]

        parquet_files = sorted(glob.glob(f'{SILVER_DIR}/transactions/**/*.parquet', recursive=True))
        log.info(f"  Found {len(parquet_files)} Parquet file(s)")

        total_staged = 0
        for filepath in parquet_files:
            pf = pq.ParquetFile(filepath)
            for batch in pf.iter_batches(batch_size=50_000):
                chunk = batch.to_pandas()
                chunk['transaction_date'] = pd.to_datetime(chunk['transaction_date']).dt.date
                chunk = chunk[chunk['transaction_date'] > last_date]
                if chunk.empty:
                    continue
                # Only keep the columns the staging table expects
                chunk = chunk[[c for c in target_cols if c in chunk.columns]]
                self._bulk_copy(chunk, 'stg_fact_transactions')
                total_staged += len(chunk)

        log.info(f"  Staged {total_staged:,} new rows")

        if total_staged == 0:
            log.info("  Nothing new to load — skipping fact INSERT")
            return

        # INSERT from staging → fact, resolving all surrogate keys in one query
        # We join on scd_is_current = TRUE to always get the latest dimension version
        sql = text("""
            INSERT INTO fact_transactions (
                date_key, account_key, customer_key, merchant_key, branch_key,
                txn_type_key, transaction_id, transaction_date, transaction_time,
                transaction_hour, amount, is_debit, balance_after, status,
                is_fraud_flag, fraud_score, fraud_risk_band, is_weekend,
                is_night_transaction, amount_band, daily_txn_count,
                daily_spend_total, reference, description, channel, dw_batch_id
            )
            SELECT
                TO_CHAR(stg.transaction_date, 'YYYYMMDD')::INT  AS date_key,
                a.account_key,
                a.customer_key,
                m.merchant_key,
                b.branch_key,
                t.txn_type_key,
                stg.transaction_id,
                stg.transaction_date,
                stg.transaction_time,
                stg.transaction_hour,
                stg.amount,
                stg.is_debit,
                stg.balance_after,
                stg.status,
                stg.is_fraud_flag,
                stg.fraud_score,
                stg.fraud_risk_band,
                stg.is_weekend,
                stg.is_night_transaction,
                stg.amount_band,
                stg.daily_txn_count,
                stg.daily_spend_total,
                stg.reference,
                stg.description,
                stg.channel,
                :batch_id
            FROM stg_fact_transactions stg
            LEFT JOIN dim_accounts          a ON stg.account_id   = a.account_id AND a.scd_is_current = TRUE
            LEFT JOIN dim_merchants         m ON stg.merchant_id  = m.merchant_id
            LEFT JOIN dim_transaction_type  t ON stg.transaction_type = t.txn_type_name
            LEFT JOIN dim_branches          b ON a.branch_key = b.branch_key
            ON CONFLICT DO NOTHING
        """)

        with self.engine.begin() as conn:
            conn.execute(sql, {'batch_id': self.batch_id})

        log.info(f"  Fact transactions updated")

    # ─────────────────────────────────────────────────────────
    # RUN ALL
    # ─────────────────────────────────────────────────────────

    def run_all(self):
        log.info("=" * 60)
        log.info("GOLD LOAD STARTING")
        log.info("=" * 60)

        self.load_branches()
        self.load_merchants()
        self.load_customers_scd2()
        self.load_accounts_scd2()
        self.load_fact_transactions()

        log.info("=" * 60)
        log.info("GOLD LOAD COMPLETE")
        log.info("=" * 60)


if __name__ == '__main__':
    loader = GoldLoader()
    loader.run_all()