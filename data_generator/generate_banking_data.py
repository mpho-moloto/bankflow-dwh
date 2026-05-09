"""
Generate synthetic South African banking data.

Used by Airflow DAG which runs daily, generating 50 customers, 100 accounts, and 5,000 transactions per run.
The script appends to existing CSV files and tracks IDs automatically.

Manual usage:
    python generate_banking_data.py --customers 50 --accounts 100 --transactions 5000
    python generate_banking_data.py --fresh

Output: data/raw/branches.csv, merchants.csv, customers.csv, accounts.csv, transactions.csv
"""

import os
import sys
import random
import argparse
import warnings
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from collections import defaultdict

# Ignore harmless pandas warnings
warnings.filterwarnings('ignore')


def parse_args():
    p = argparse.ArgumentParser(description='Generate SA banking data')
    p.add_argument('--transactions', type=int, default=5000,
                   help='How many transactions to create')
    p.add_argument('--customers', type=int, default=50,
                   help='How many customers to create')
    p.add_argument('--accounts', type=int, default=100,
                   help='How many bank accounts to create')
    p.add_argument('--seed', type=int, default=None,
                   help='Set a specific random seed for reproducible results')
    p.add_argument('--fresh', action='store_true',
                   help='Delete all existing data before generating new data')
    return p.parse_args()


args = parse_args()

# Set up randomness so results can be reproduced
seed = args.seed if args.seed is not None else random.randint(0, 99999)
Faker.seed(seed)
random.seed(seed)
np.random.seed(seed)

# South African locale for realistic names, phone numbers, etc.
try:
    fake = Faker(['en_ZA', 'en_US'])
except Exception:
    fake = Faker()

# Where CSV files will be saved
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# South Africa's 9 provinces
SA_PROVINCES = [
    'Gauteng', 'Western Cape', 'KwaZulu-Natal', 'Eastern Cape',
    'Free State', 'Limpopo', 'Mpumalanga', 'North West', 'Northern Cape'
]

# Population weights - Gauteng gets 30% of customers, Northern Cape only 3%
PROVINCE_WEIGHTS = [0.30, 0.18, 0.15, 0.10, 0.06, 0.07, 0.06, 0.05, 0.03]

# Major cities per province
SA_CITIES = {
    'Gauteng':       ['Johannesburg', 'Pretoria', 'Sandton', 'Soweto', 'Centurion', 'Midrand'],
    'Western Cape':  ['Cape Town', 'Stellenbosch', 'George', 'Paarl', 'Somerset West'],
    'KwaZulu-Natal': ['Durban', 'Pietermaritzburg', 'Richards Bay', 'Newcastle'],
    'Eastern Cape':  ['Gqeberha', 'East London', 'Mthatha', 'Uitenhage'],
    'Free State':    ['Bloemfontein', 'Welkom', 'Phuthaditjhaba'],
    'Limpopo':       ['Polokwane', 'Tzaneen', 'Louis Trichardt'],
    'Mpumalanga':    ['Mbombela', 'Witbank', 'Middelburg'],
    'North West':    ['Rustenburg', 'Mahikeng', 'Potchefstroom'],
    'Northern Cape': ['Kimberley', 'Upington', 'Springbok']
}

# Customer segments and how common each is (55% are Retail)
CUSTOMER_SEGMENTS = ['Retail', 'Private Banking', 'Business', 'Youth', 'Pensioner']
SEGMENT_WEIGHTS   = [0.55, 0.10, 0.20, 0.10, 0.05]

ACCOUNT_TYPES    = ['Cheque', 'Savings', 'Credit Card', 'Fixed Deposit', 'Business Current']
ACCOUNT_STATUSES = ['Active', 'Active', 'Active', 'Active', 'Dormant', 'Closed']

# Transaction types weighted by frequency (Purchase appears 4 times = 40% of all txns)
TRANSACTION_TYPES = [
    'Purchase', 'Purchase', 'Purchase', 'Purchase',
    'ATM Withdrawal', 'ATM Withdrawal',
    'EFT Transfer Out', 'EFT Transfer In',
    'Debit Order', 'Debit Order',
    'Salary Credit',
    'Bank Charges',
    'Reversal',
]

# Mobile App appears twice because it's the most common channel
CHANNELS = ['Mobile App', 'Mobile App', 'Internet Banking', 'ATM', 'Branch', 'POS']

# Real South African merchants with their categories
SA_MERCHANTS = [
    ('Pick n Pay', 'Grocery'), ('Checkers', 'Grocery'), ('Woolworths Food', 'Grocery'),
    ('Shoprite', 'Grocery'), ('Spar', 'Grocery'), ("Food Lover's Market", 'Grocery'),
    ('Engen', 'Fuel'), ('Shell', 'Fuel'), ('BP', 'Fuel'), ('Total', 'Fuel'),
    ('KFC', 'Fast Food'), ("McDonald's", 'Fast Food'), ('Steers', 'Fast Food'),
    ("Nando's", 'Fast Food'), ('Chicken Licken', 'Fast Food'), ('Debonairs', 'Fast Food'),
    ('Edgars', 'Clothing'), ('Truworths', 'Clothing'), ('Mr Price', 'Clothing'),
    ('Pep', 'Clothing'), ('Ackermans', 'Clothing'), ('Exact', 'Clothing'),
    ('Makro', 'Electronics'), ('Game', 'Electronics'), ('Hi-Fi Corp', 'Electronics'),
    ('Incredible Connection', 'Electronics'),
    ('Clicks', 'Pharmacy'), ('Dischem', 'Pharmacy'), ('Medirite', 'Pharmacy'),
    ('Eskom', 'Utilities'), ('City of Johannesburg', 'Utilities'),
    ('Telkom', 'Utilities'), ('MTN', 'Telecoms'), ('Vodacom', 'Telecoms'),
    ('Capitec Bank ATM', 'ATM'), ('FNB ATM', 'ATM'), ('Standard Bank ATM', 'ATM'),
    ('Ster-Kinekor', 'Entertainment'), ('Nu Metro', 'Entertainment'),
    ('Spotify', 'Entertainment'), ('Netflix SA', 'Entertainment'),
    ('Uber SA', 'Transport'), ('Bolt SA', 'Transport'), ('Gautrain', 'Transport'),
    ('Builders Warehouse', 'Home & Garden'), ('Leroy Merlin', 'Home & Garden'),
]


def maybe_reset():
    """Delete all existing CSV files if --fresh flag is used"""
    if not args.fresh:
        return
    print("--fresh flag set. Deleting existing CSV files...")
    files_to_delete = ['branches.csv', 'merchants.csv', 'customers.csv', 
                       'accounts.csv', 'transactions.csv']
    for filename in files_to_delete:
        filepath = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"  Deleted {filename}")


def save_csv(dataframe, filename, id_column=None):
    """Save or append CSV. Uses id_column to skip duplicates."""
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    # New file - just write it
    if not os.path.exists(filepath):
        dataframe.to_csv(filepath, index=False)
        print(f"  {filename}: wrote {len(dataframe):,} rows (new file)")
        return
    
    # No ID column provided - overwrite
    if id_column is None:
        dataframe.to_csv(filepath, index=False)
        return
    
    # Append only rows with new IDs
    existing_data = pd.read_csv(filepath, dtype=str) 
    new_rows = dataframe[~dataframe[id_column].isin(existing_data[id_column])]
    
    if new_rows.empty:
        print(f"  {filename}: no new rows to append (all IDs already exist)")
        return
    
    combined = pd.concat([existing_data, new_rows], ignore_index=True)
    combined.to_csv(filepath, index=False)
    print(f"  {filename}: appended {len(new_rows):,} rows (total: {len(combined):,})")


def generate_branches(n=50):
    """Create branch records across SA provinces, weighted by population"""
    rows = []
    branch_id = 1000
    
    for idx, province in enumerate(SA_PROVINCES):
        cities = SA_CITIES[province]
        # More branches in provinces with higher population
        num_branches = max(2, int(n * PROVINCE_WEIGHTS[idx]))
        
        for _ in range(num_branches):
            city = random.choice(cities)
            # Major cities are 'Urban', others 'Suburban'
            is_major = city in ['Johannesburg', 'Cape Town', 'Durban', 'Pretoria']
            
            rows.append({
                'branch_id':   f'BR{branch_id}',
                'branch_name': f'{city} {random.choice(["Main", "Central", "North", "South", "Mall"])}',
                'city':        city,
                'province':    province,
                'region':      'Urban' if is_major else 'Suburban',
                'opened_date': fake.date_between(start_date='-20y', end_date='-2y').isoformat(),
                'is_active':   True,
            })
            branch_id += 1
    
    df = pd.DataFrame(rows).drop_duplicates('branch_id').head(n)
    save_csv(df, 'branches.csv', id_column='branch_id')
    return df


def generate_merchants():
    """Create 200 merchants (real SA brands + fake ones)"""
    rows = []
    
    # First add all real SA merchants
    for idx, (name, category) in enumerate(SA_MERCHANTS, start=1):
        rows.append({
            'merchant_id':       f'MER{idx:04d}',
            'merchant_name':     name,
            'merchant_category': category,
            'mcc_code':          random.randint(1000, 9999),
            'is_online':         name in ['Spotify', 'Netflix SA', 'Uber SA', 'Bolt SA'],
        })
    
    # Fill up to 200 with fake merchants
    for idx in range(len(SA_MERCHANTS) + 1, 201):
        rows.append({
            'merchant_id':       f'MER{idx:04d}',
            'merchant_name':     fake.company() + ' SA',
            'merchant_category': random.choice(['Online Services', 'Insurance', 'Medical', 'Education']),
            'mcc_code':          random.randint(1000, 9999),
            'is_online':         True,
        })
    
    df = pd.DataFrame(rows)
    save_csv(df, 'merchants.csv', id_column='merchant_id')
    return df


def generate_customers(n=50):
    """Generate customers with realistic SA ID numbers and age-appropriate segments"""
    filepath = os.path.join(OUTPUT_DIR, 'customers.csv')
    
    # Find next available customer ID for appending
    start_idx = 1
    if os.path.exists(filepath):
        existing = pd.read_csv(filepath, dtype=str)
        last_id = existing['customer_id'].str.replace('CUS', '').astype(int).max()
        start_idx = last_id + 1
    
    rows = []
    
    for idx in range(start_idx, start_idx + n):
        province = np.random.choice(SA_PROVINCES, p=PROVINCE_WEIGHTS)
        city = random.choice(SA_CITIES[province])
        segment = np.random.choice(CUSTOMER_SEGMENTS, p=SEGMENT_WEIGHTS)
        
        # Age ranges that make sense for each segment
        if segment == 'Youth':
            age = random.randint(18, 28)
        elif segment == 'Pensioner':
            age = random.randint(60, 85)
        elif segment == 'Private Banking':
            age = random.randint(35, 65)
        else:
            age = random.randint(25, 60)
        
        dob = datetime.now() - timedelta(days=age * 365 + random.randint(0, 364))
        gender_digit = random.choice([0, 1])  # 0=Female, 1=Male
        # Generate SA ID number: YYMMDD + sequence + gender + 8 + checksum
        sa_id = (dob.strftime('%y%m%d') + str(random.randint(5000, 9999)) + 
                 str(gender_digit) + '8' + str(random.randint(0, 9)))
        
        rows.append({
            'customer_id':       f'CUS{idx:06d}',
            'first_name':        fake.first_name(),
            'last_name':         fake.last_name(),
            'email':             fake.email(),
            'phone':             fake.phone_number(),
            'id_number':         sa_id,
            'date_of_birth':     dob.strftime('%Y-%m-%d'),
            'age':               age,
            'gender':            'Male' if gender_digit == 1 else 'Female',
            'province':          province,
            'city':              city,
            'postal_code':       fake.postcode(),
            'customer_segment':  segment,
            'income_band':       _get_income_band(segment),
            'risk_rating':       random.choice(['Low', 'Low', 'Medium', 'Medium', 'High']),
            'kyc_verified':      random.choice([True, True, True, False]),  # 75% verified
            'onboarding_date':   fake.date_between(start_date='-8y', end_date='-1m').isoformat(),
            'is_active':         random.choice([True, True, True, True, False]),  # 80% active
        })
    
    df = pd.DataFrame(rows)
    save_csv(df, 'customers.csv', id_column='customer_id')
    return df


def _get_income_band(segment):
    """Return realistic income range based on customer segment"""
    income_bands = {
        'Retail':          ['R0-R10k', 'R10k-R25k', 'R25k-R50k'],
        'Private Banking': ['R50k-R100k', 'R100k+'],
        'Business':        ['R25k-R50k', 'R50k-R100k', 'R100k+'],
        'Youth':           ['R0-R10k', 'R10k-R25k'],
        'Pensioner':       ['R0-R10k', 'R10k-R25k', 'R25k-R50k'],
    }
    return random.choice(income_bands.get(segment, ['R10k-R25k']))


def generate_accounts(customers_df, branches_df, n=100):
    """Generate bank accounts linked to customers. Credit cards get limits, cheques get overdrafts."""
    filepath = os.path.join(OUTPUT_DIR, 'accounts.csv')
    
    # Find next available account ID
    start_idx = 1
    if os.path.exists(filepath):
        existing = pd.read_csv(filepath, dtype=str)
        last_id = existing['account_id'].str.replace('ACC', '').astype(int).max()
        start_idx = last_id + 1
    
    customer_ids = customers_df['customer_id'].tolist()
    branch_ids = branches_df['branch_id'].tolist()
    rows = []
    
    for idx in range(start_idx, start_idx + n):
        customer_id = random.choice(customer_ids)
        account_type = random.choice(ACCOUNT_TYPES)
        status = random.choice(ACCOUNT_STATUSES)
        
        # Different account types have different balance rules
        if account_type == 'Fixed Deposit':
            opening_balance = round(random.uniform(10_000, 500_000), 2)
            credit_limit = None
        elif account_type == 'Credit Card':
            opening_balance = 0.0
            credit_limit = random.choice([5_000, 10_000, 20_000, 50_000, 100_000])
        elif account_type == 'Business Current':
            opening_balance = round(random.uniform(5_000, 200_000), 2)
            credit_limit = None
        else:  # Cheque or Savings
            opening_balance = round(random.uniform(0, 50_000), 2)
            credit_limit = None
        
        open_date = fake.date_between(start_date='-7y', end_date='-3m')
        current_balance = round(opening_balance + random.uniform(-5_000, 50_000), 2)
        
        # Non-credit accounts can't go negative
        if account_type != 'Credit Card' and current_balance < 0:
            current_balance = max(0, current_balance)
        
        rows.append({
            'account_id':       f'ACC{idx:07d}',
            'customer_id':      customer_id,
            'branch_id':        random.choice(branch_ids),
            'account_type':     account_type,
            'account_number':   str(random.randint(1_000_000_000, 9_999_999_999)),
            'currency':         'ZAR',
            'opening_balance':  opening_balance,
            'current_balance':  current_balance,
            'credit_limit':     credit_limit,
            'interest_rate':    _get_interest_rate(account_type),
            'status':           status,
            'open_date':        open_date.isoformat(),
            'close_date':       fake.date_between(start_date=open_date, end_date='today').isoformat() if status == 'Closed' else None,
            'overdraft_limit':  round(random.uniform(500, 5_000), 2) if account_type == 'Cheque' else 0.0,
        })
    
    df = pd.DataFrame(rows)
    save_csv(df, 'accounts.csv', id_column='account_id')
    return df


def _get_interest_rate(account_type):
    """Credit cards have high rates (you pay), savings have low rates (bank pays you)"""
    rates = {
        'Cheque':           round(random.uniform(0.0, 2.5), 2),
        'Savings':          round(random.uniform(4.0, 7.5), 2),
        'Credit Card':      round(random.uniform(18.0, 22.5), 2),
        'Fixed Deposit':    round(random.uniform(8.0, 11.5), 2),
        'Business Current': round(random.uniform(0.0, 3.0), 2),
    }
    return rates.get(account_type, 0.0)


def generate_transactions(accounts_df, merchants_df, n=5000):
    """Generate transaction history. Handles declines, overdraft fees, and fraud flags."""
    filepath = os.path.join(OUTPUT_DIR, 'transactions.csv')
    
    # Find next transaction ID
    start_idx = 1
    if os.path.exists(filepath):
        existing = pd.read_csv(filepath, dtype=str)
        last_id = existing['transaction_id'].str.replace('TXN', '').astype(int).max()
        start_idx = last_id + 1
    
    # Only active accounts can transact
    active_accounts = accounts_df[accounts_df['status'] == 'Active']
    active_account_ids = active_accounts['account_id'].tolist()
    merchant_ids = merchants_df['merchant_id'].tolist()
    
    # Quick lookup dict for account details (faster than filtering DataFrame each time)
    account_details = {}
    for _, row in active_accounts.iterrows():
        account_details[row['account_id']] = {
            'account_type':    row['account_type'],
            'current_balance': float(row['current_balance']),
            'overdraft_limit': float(row['overdraft_limit']) if row['overdraft_limit'] else 0,
            'credit_limit':    float(row['credit_limit']) if row['credit_limit'] else 0,
        }
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)  # Last 2 years
    
    # Track running balance as we generate
    running_balance = {acc_id: account_details[acc_id]['current_balance'] 
                       for acc_id in active_account_ids}
    
    rows = []
    declined_count = 0
    overdraft_fee_count = 0
    BATCH_SIZE = 10_000
    
    print(f"  Generating {n:,} transactions...")
    
    for i in range(start_idx, start_idx + n):
        account_id = random.choice(active_account_ids)
        account_info = account_details[account_id]
        
        transaction_date = _get_weighted_timestamp(start_date, end_date)
        is_weekend = transaction_date.weekday() >= 5
        is_december = transaction_date.month == 12
        
        transaction_type = _select_transaction_type(is_december, is_weekend)
        channel = _select_channel(transaction_type, is_weekend)
        amount = _calculate_amount(transaction_type, is_december, is_weekend)
        is_debit = transaction_type not in ('Salary Credit', 'EFT Transfer In', 'Reversal')
        is_declined = _check_if_declined(account_info, transaction_type, amount, 
                                         running_balance[account_id])
        
        signed_amount = -amount if is_debit else amount
        
        if not is_declined:
            new_balance = running_balance[account_id] + signed_amount
            
            # Overdraft fee for cheque accounts going negative
            if new_balance < 0 and account_info['account_type'] == 'Cheque':
                if abs(new_balance) <= account_info['overdraft_limit']:
                    new_balance -= round(random.uniform(50, 150), 2)
                    overdraft_fee_count += 1
                else:
                    is_declined = True
            
            if not is_declined:
                running_balance[account_id] = round(new_balance, 2)
        
        if is_declined:
            declined_count += 1
            status = random.choice([
                'Declined - Insufficient Funds', 
                'Declined - Over Limit',
                'Declined - Blocked', 
                'Failed'
            ])
        else:
            status = random.choice(['Completed', 'Completed', 'Completed', 'Pending'])
        
        # 0.5% of transactions flagged as fraud
        is_fraud = random.random() < 0.005
        fraud_score = round(random.uniform(0.7, 1.0), 3) if is_fraud else round(random.uniform(0.0, 0.3), 3)
        
        rows.append({
            'transaction_id':   f'TXN{i:09d}',
            'account_id':       account_id,
            'merchant_id':      random.choice(merchant_ids) if transaction_type == 'Purchase' else None,
            'transaction_type': transaction_type,
            'channel':          channel,
            'amount':           amount,
            'is_debit':         is_debit,
            'currency':         'ZAR',
            'balance_after':    running_balance[account_id],
            'transaction_date': transaction_date.strftime('%Y-%m-%d'),
            'transaction_time': transaction_date.strftime('%H:%M:%S'),
            'reference':        fake.uuid4()[:8].upper(),
            'description':      _get_transaction_description(transaction_type),
            'status':           status,
            'is_fraud_flag':    is_fraud,
            'fraud_score':      fraud_score,
            'is_weekend':       is_weekend,
            'is_holiday_season': is_december,
        })
        
        if (i - start_idx + 1) % BATCH_SIZE == 0:
            percent_done = (i - start_idx + 1) / n * 100
            print(f"  Progress: {i - start_idx + 1:,} / {n:,} ({percent_done:.0f}%)")
    
    df = pd.DataFrame(rows)
    df.sort_values(['transaction_date', 'transaction_time'], inplace=True)
    save_csv(df, 'transactions.csv', id_column='transaction_id')
    
    print(f"\n  Transaction summary for this batch:")
    print(f"    Total generated : {len(df):,}")
    print(f"    Declined        : {declined_count:,} ({declined_count / max(len(df), 1) * 100:.1f}%)")
    print(f"    Overdraft fees  : {overdraft_fee_count:,}")
    
    return df


def _get_weighted_timestamp(start_date, end_date):
    """Pick random timestamp, but bias toward business hours (9am-5pm)"""
    total_seconds = int((end_date - start_date).total_seconds())
    random_seconds = random.randint(0, total_seconds)
    transaction_time = start_date + timedelta(seconds=random_seconds)
    hour = transaction_time.hour
    
    # Late night - move to business hours 70% of the time
    if hour < 6 or hour >= 22:
        if random.random() < 0.7:
            transaction_time = transaction_time.replace(hour=random.randint(9, 17))
    # Evening - move to business hours 30% of the time
    elif 18 <= hour <= 22:
        if random.random() < 0.3:
            transaction_time = transaction_time.replace(hour=random.randint(9, 17))
    
    return transaction_time


def _select_transaction_type(is_december, is_weekend):
    """Choose transaction type. More purchases in December and on weekends."""
    transaction_types = TRANSACTION_TYPES.copy()
    
    if is_december:
        transaction_types += ['Purchase'] * 3  # Extra holiday shopping
        # Fewer debit orders in December (people pause subscriptions)
        transaction_types = [t for t in transaction_types if not (t == 'Debit Order' and random.random() < 0.3)]
    
    if is_weekend:
        transaction_types += ['Purchase'] * 2 + ['ATM Withdrawal'] * 2
        # Fewer business transactions on weekends
        transaction_types = [t for t in transaction_types if not (t in ['EFT Transfer Out', 'Debit Order'] and random.random() < 0.4)]
    
    return random.choice(transaction_types)


def _select_channel(transaction_type, is_weekend):
    """Determine how transaction was performed (mobile app, ATM, etc.)"""
    if transaction_type == 'Purchase':
        return 'POS'
    if transaction_type == 'ATM Withdrawal':
        return 'ATM'
    
    channels = CHANNELS.copy()
    
    if is_weekend:
        channels += ['Mobile App'] * 3  # More mobile banking on weekends
        channels = [c for c in channels if c != 'Branch']  # Branches closed
    
    return random.choice(channels)


def _calculate_amount(transaction_type, is_december, is_weekend):
    """Return amount based on transaction type. Holiday and weekend boosts apply."""
    amount_ranges = {
        'Salary Credit':    random.uniform(8_000, 80_000),
        'ATM Withdrawal':   random.choice([200, 500, 1_000, 2_000, 3_000]),
        'Debit Order':      random.uniform(100, 5_000),
        'Bank Charges':     random.uniform(5, 150),
        'Purchase':         random.uniform(10, 5_000),
        'EFT Transfer Out': random.uniform(100, 30_000),
        'EFT Transfer In':  random.uniform(100, 30_000),
        'Reversal':         random.uniform(10, 1_000),
    }
    
    amount = amount_ranges.get(transaction_type, random.uniform(10, 1_000))
    
    # 30-50% more spending in December
    if is_december and transaction_type == 'Purchase':
        amount *= random.uniform(1.3, 1.5)
    
    # 15-25% more spending on weekends
    if is_weekend and transaction_type in ['Purchase', 'ATM Withdrawal']:
        amount *= random.uniform(1.15, 1.25)
    
    return round(amount, 2)


def _check_if_declined(account_info, transaction_type, amount, current_balance):
    """Determine if transaction should be declined (insufficient funds, over limit, etc.)"""
    # Random technical failure (2% of transactions)
    if random.random() < 0.02:
        return True
    
    # Money coming in is never declined
    is_debit = transaction_type not in ('Salary Credit', 'EFT Transfer In', 'Reversal')
    if not is_debit:
        return False
    
    # Credit card - check credit limit
    if account_info['account_type'] == 'Credit Card' and account_info['credit_limit'] > 0:
        return amount > (account_info['credit_limit'] - abs(current_balance))
    
    # Cheque account - can use balance + overdraft
    if account_info['account_type'] == 'Cheque':
        return amount > (current_balance + account_info['overdraft_limit'])
    
    # All other accounts - can't spend more than you have
    return amount > current_balance


def _get_transaction_description(transaction_type):
    """Human-readable description for bank statement"""
    descriptions = {
        'Purchase':         random.choice(['Card Purchase', 'POS Purchase', 'Online Purchase']),
        'ATM Withdrawal':   'ATM Cash Withdrawal',
        'EFT Transfer Out': 'Electronic Funds Transfer',
        'EFT Transfer In':  'Incoming EFT',
        'Debit Order':      random.choice(['Debit Order - Insurance', 'Debit Order - Rent', 'Debit Order - Vehicle']),
        'Salary Credit':    'Salary Payment',
        'Bank Charges':     random.choice(['Monthly Fee', 'Transaction Fee', 'Overdraft Fee']),
        'Reversal':         'Transaction Reversal',
    }
    return descriptions.get(transaction_type, 'General Transaction')


if __name__ == '__main__':
    maybe_reset()
    
    print(f"\nUsing random seed: {seed}")
    print("Generating data...\n")
    
    branches_df = generate_branches(n=50)
    merchants_df = generate_merchants()
    customers_df = generate_customers(n=args.customers)
    accounts_df = generate_accounts(customers_df, branches_df, n=args.accounts)
    transactions_df = generate_transactions(accounts_df, merchants_df, n=args.transactions)
    
    print("\n" + "=" * 60)
    print("DATA GENERATION COMPLETE")
    print(f"  Files saved to: {OUTPUT_DIR}")
    print("=" * 60)