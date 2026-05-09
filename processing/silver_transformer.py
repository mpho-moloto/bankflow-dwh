"""
Silver layer: type casting, deduplication, derived columns, and window functions.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, DateType

BRONZE_DIR = '/opt/spark/data/bronze'
SILVER_DIR = '/opt/spark/data/silver'


def build_spark():
    return (
        SparkSession.builder
        .appName('BankFlow-Silver')
        .master(os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077'))
        .config('spark.sql.adaptive.enabled', 'true')
        .config('spark.sql.shuffle.partitions', '8')
        .config('spark.sql.parquet.compression.codec', 'snappy')
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# BRANCHES
# ─────────────────────────────────────────────────────────────

def transform_branches(spark):
    print("\n[1/5] branches...")
    df = spark.read.parquet(f'{BRONZE_DIR}/branches')

    silver = (
        df.select(
            F.col('branch_id').cast(StringType()),
            F.col('branch_name').cast(StringType()),
            F.col('city').cast(StringType()),
            F.col('province').cast(StringType()),
            F.col('region').cast(StringType()),
            F.to_date('opened_date', 'yyyy-MM-dd').alias('opened_date'),
            F.col('is_active').cast(BooleanType()),
            F.col('_ingestion_ts'),
            F.col('_batch_id'),
        )
        .dropDuplicates(['branch_id'])
        .filter(F.col('branch_id').isNotNull())
    )

    _write(silver, 'branches')
    print(f"  {silver.count():,} branches written")


# ─────────────────────────────────────────────────────────────
# MERCHANTS
# ─────────────────────────────────────────────────────────────

def transform_merchants(spark):
    print("\n[2/5] merchants...")
    df = spark.read.parquet(f'{BRONZE_DIR}/merchants')

    silver = (
        df.select(
            F.col('merchant_id').cast(StringType()),
            F.col('merchant_name').cast(StringType()),
            F.col('merchant_category').cast(StringType()),
            F.col('mcc_code').cast(IntegerType()),
            F.col('is_online').cast(BooleanType()),
            F.col('_ingestion_ts'),
            F.col('_batch_id'),
        )
        .dropDuplicates(['merchant_id'])
        .filter(F.col('merchant_id').isNotNull())
    )

    _write(silver, 'merchants')
    print(f"  {silver.count():,} merchants written")


# ─────────────────────────────────────────────────────────────
# CUSTOMERS
# ─────────────────────────────────────────────────────────────

def transform_customers(spark):
    print("\n[3/5] customers...")
    df = spark.read.parquet(f'{BRONZE_DIR}/customers')

    silver = (
        df.select(
            F.col('customer_id').cast(StringType()),
            F.col('first_name').cast(StringType()),
            F.col('last_name').cast(StringType()),
            F.concat_ws(' ', F.col('first_name'), F.col('last_name')).alias('full_name'),
            F.col('email').cast(StringType()),
            F.col('phone').cast(StringType()),
            F.to_date('date_of_birth', 'yyyy-MM-dd').alias('date_of_birth'),
            F.col('age').cast(IntegerType()),
            # Bucket age into bands
            (F.when(F.col('age') < 25, 'Under 25')
              .when(F.col('age') < 35, '25-34')
              .when(F.col('age') < 45, '35-44')
              .when(F.col('age') < 55, '45-54')
              .when(F.col('age') < 65, '55-64')
              .otherwise('65+')).alias('age_band'),
            F.col('gender').cast(StringType()),
            F.col('province').cast(StringType()),
            F.col('city').cast(StringType()),
            F.col('postal_code').cast(StringType()),
            F.col('customer_segment').cast(StringType()),
            F.col('income_band').cast(StringType()),
            F.col('risk_rating').cast(StringType()),
            F.col('kyc_verified').cast(BooleanType()),
            F.to_date('onboarding_date', 'yyyy-MM-dd').alias('onboarding_date'),
            F.col('is_active').cast(BooleanType()),
            F.col('_ingestion_ts'),
            F.col('_batch_id'),
        )
        .dropDuplicates(['customer_id'])
        .filter(F.col('customer_id').isNotNull())
        .filter(F.col('email').contains('@'))
    )

    _write(silver, 'customers')
    print(f"  {silver.count():,} customers written")


# ─────────────────────────────────────────────────────────────
# ACCOUNTS
# ─────────────────────────────────────────────────────────────

def transform_accounts(spark):
    print("\n[4/5] accounts...")
    df = spark.read.parquet(f'{BRONZE_DIR}/accounts')

    silver = (
        df.select(
            F.col('account_id').cast(StringType()),
            F.col('customer_id').cast(StringType()),
            F.col('branch_id').cast(StringType()),
            F.col('account_type').cast(StringType()),
            F.col('account_number').cast(StringType()),
            F.col('currency').cast(StringType()),
            F.col('opening_balance').cast(DoubleType()),
            F.col('current_balance').cast(DoubleType()),
            F.col('credit_limit').cast(DoubleType()),
            F.col('interest_rate').cast(DoubleType()),
            F.col('status').cast(StringType()),
            F.to_date('open_date', 'yyyy-MM-dd').alias('open_date'),
            F.to_date('close_date', 'yyyy-MM-dd').alias('close_date'),
            F.col('overdraft_limit').cast(DoubleType()),
            # Account age in days
            F.datediff(F.current_date(),
                       F.to_date('open_date', 'yyyy-MM-dd')).alias('account_age_days'),
            F.when(F.col('overdraft_limit').cast(DoubleType()) > 0, True)
             .otherwise(False).alias('has_overdraft'),
        )
        .dropDuplicates(['account_id'])
        .filter(F.col('account_id').isNotNull())
        .filter(F.col('customer_id').isNotNull())
    )

    _write(silver, 'accounts')
    print(f"  {silver.count():,} accounts written")


# ─────────────────────────────────────────────────────────────
# TRANSACTIONS
# ─────────────────────────────────────────────────────────────

def transform_transactions(spark):
    print("\n[5/5] transactions (this will take a while)...")
    df = spark.read.parquet(f'{BRONZE_DIR}/transactions')

    # Cast all columns
    typed = df.select(
        F.col('transaction_id').cast(StringType()),
        F.col('account_id').cast(StringType()),
        F.col('merchant_id').cast(StringType()),
        F.col('transaction_type').cast(StringType()),
        F.col('channel').cast(StringType()),
        F.col('amount').cast(DoubleType()),
        F.col('is_debit').cast(BooleanType()),
        F.col('currency').cast(StringType()),
        F.col('balance_after').cast(DoubleType()),
        F.to_date('transaction_date', 'yyyy-MM-dd').alias('transaction_date'),
        F.col('transaction_time').cast(StringType()),
        F.col('reference').cast(StringType()),
        F.col('description').cast(StringType()),
        F.col('status').cast(StringType()),
        F.col('is_fraud_flag').cast(BooleanType()),
        F.col('fraud_score').cast(DoubleType()),
        F.col('is_weekend').cast(BooleanType()),
        F.col('is_holiday_season').cast(BooleanType()),
        F.col('_ingestion_ts'),
        F.col('_batch_id'),
    )

    # Temporal derived columns
    enriched = (
        typed
        .withColumn('transaction_hour',
                    F.hour(F.to_timestamp('transaction_time', 'HH:mm:ss')))
        .withColumn('transaction_day_of_week',
                    F.dayofweek('transaction_date'))
        .withColumn('transaction_month',
                    F.month('transaction_date'))
        .withColumn('transaction_year',
                    F.year('transaction_date'))
        .withColumn('is_night_transaction',
                    (F.col('transaction_hour') < 6) | (F.col('transaction_hour') >= 22))
        .withColumn('is_business_hours',
                    (F.col('transaction_hour') >= 9) & (F.col('transaction_hour') <= 17))
    )

    # Banding
    enriched = (
        enriched
        .withColumn('amount_band',
                    F.when(F.col('amount') < 100,    'Under R100')
                     .when(F.col('amount') < 500,    'R100-R500')
                     .when(F.col('amount') < 1_000,  'R500-R1k')
                     .when(F.col('amount') < 5_000,  'R1k-R5k')
                     .when(F.col('amount') < 20_000, 'R5k-R20k')
                     .otherwise('Over R20k'))
        .withColumn('fraud_risk_band',
                    F.when(F.col('fraud_score') < 0.3, 'Low')
                     .when(F.col('fraud_score') < 0.6, 'Medium')
                     .when(F.col('fraud_score') < 0.8, 'High')
                     .otherwise('Critical'))
        .withColumn('is_declined',
                    F.col('status').contains('Declined') | (F.col('status') == 'Failed'))
        .withColumn('is_completed', F.col('status') == 'Completed')
    )

    # Daily velocity (completed only, then left-joined back)
    w_daily = (
        Window
        .partitionBy('account_id', 'transaction_date')
        .orderBy('transaction_id')
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    completed = enriched.filter(F.col('is_completed'))

    with_velocity = (
        completed
        .withColumn('daily_txn_count',
                    F.count('transaction_id').over(w_daily))
        .withColumn('daily_spend_total',
                    F.sum(F.when(F.col('is_debit'), F.col('amount')).otherwise(0)).over(w_daily))
        .select('transaction_id', 'daily_txn_count', 'daily_spend_total')
    )

    final = (
        enriched
        .join(with_velocity, on='transaction_id', how='left')
        .fillna({'daily_txn_count': 0, 'daily_spend_total': 0.0})
        .dropDuplicates(['transaction_id'])
        .filter(F.col('transaction_id').isNotNull())
        .filter(F.col('amount') > 0)
    )

    # Write partitioned by year/month
    (
        final.write
        .mode('overwrite')
        .partitionBy('transaction_year', 'transaction_month')
        .parquet(f'{SILVER_DIR}/transactions')
    )

    total     = final.count()
    completed = final.filter(F.col('is_completed')).count()
    declined  = final.filter(F.col('is_declined')).count()

    print(f"  {total:,} transactions written")
    print(f"    Completed : {completed:,}")
    print(f"    Declined  : {declined:,} ({declined / max(total, 1) * 100:.1f}%)")
    print(f"    Partitioned by year / month")


# ─────────────────────────────────────────────────────────────
# REFERENTIAL INTEGRITY CHECK
# ─────────────────────────────────────────────────────────────

def check_integrity(spark):
    print("\nReferential integrity checks...")

    customers    = spark.read.parquet(f'{SILVER_DIR}/customers')
    accounts     = spark.read.parquet(f'{SILVER_DIR}/accounts')
    transactions = spark.read.parquet(f'{SILVER_DIR}/transactions')
    branches     = spark.read.parquet(f'{SILVER_DIR}/branches')

    orphan_accts = accounts.join(customers, 'customer_id', 'left_anti').count()
    orphan_txns  = transactions.join(accounts, 'account_id', 'left_anti').count()
    orphan_br    = accounts.join(branches, 'branch_id', 'left_anti').count()

    if orphan_accts: print(f"  WARNING: {orphan_accts:,} accounts with no customer")
    if orphan_txns:  print(f"  WARNING: {orphan_txns:,} transactions with no account")
    if orphan_br:    print(f"  WARNING: {orphan_br:,} accounts with no branch")
    if not any([orphan_accts, orphan_txns, orphan_br]):
        print("  All checks passed!")


# ─────────────────────────────────────────────────────────────
# WRITE HELPER
# ─────────────────────────────────────────────────────────────

def _write(df, table: str):
    df.write.mode('overwrite').parquet(f'{SILVER_DIR}/{table}')


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == '__main__':
    spark = build_spark()
    spark.sparkContext.setLogLevel('WARN')

    print("=" * 60)
    print("SILVER TRANSFORMATION STARTING")
    print("=" * 60)

    transform_branches(spark)
    transform_merchants(spark)
    transform_customers(spark)
    transform_accounts(spark)
    transform_transactions(spark)
    check_integrity(spark)

    print("\n" + "=" * 60)
    print("SILVER DONE")
    print("=" * 60)

    spark.stop()