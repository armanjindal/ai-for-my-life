from dataclasses import dataclass
import requests
import os
import datetime
from urllib.parse import urlparse
from src.services.db import  update_transactions_table, update_account_snapshots_table, update_accounts_table

# 1. Get a Setup Token
from dotenv import load_dotenv
import logfire

logfire.configure()

logfire.info("Starting SimpleFin Chase Transactions Sync")
load_dotenv()


SIMPLEFIN_ACCESS_URL = os.getenv('SIMPLEFIN_ACCESS_URL')



parsed_url = urlparse(SIMPLEFIN_ACCESS_URL)
# parsed_url.netloc will be 'username:password@host'
if '@' in parsed_url.netloc:
    auth, netloc = parsed_url.netloc.split('@', 1)
    username, password = auth.split(':', 1)
else:
    raise ValueError("SIMPLEFIN_ACCESS_URL missing credentials in netloc")
# Rebuild the URL without credentials, append /accounts
url = f"{parsed_url.scheme}://{netloc}/simplefin/accounts"
print(username, password)
print(url)
# Create a daily time intervals
yesterday = datetime.datetime.now() - datetime.timedelta(days=2)
start_date_ts = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
end_date_ts = int(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

query_params = {
    'pending': '1',
    'start-date': str(start_date_ts),
    'end-date': str(end_date_ts),
}

response = requests.get(url, auth=(username, password), params=query_params)

data = response.json()

@dataclass
class SyncStatistics:
    total_transactions: int = 0

def sync_account_from_api(account_data: dict) -> None:
    sync_statistics = SyncStatistics()
    """
    Sync a single account from SimpleFin API response
    
    Args:
        account_data: Account dict from SimpleFin API
    """
    # Update account
    update_accounts_table(
        account_id=account_data['id'],
        name=account_data['name'],
        currency=account_data['currency'],
        balance=float(account_data['balance']),
        available_balance=float(account_data['available-balance']),
        balance_date=account_data['balance-date']
    )
    
    # Insert snapshot
    update_account_snapshots_table(
        account_id=account_data['id'],
        balance=float(account_data['balance']),
        available_balance=float(account_data['available-balance']),
        balance_date=account_data['balance-date']
    )
    
    # Insert transactions
    if 'transactions' in account_data and account_data['transactions']:
        sync_statistics.total_transactions += len(account_data['transactions'])
        update_transactions_table(
            account_id=account_data['id'],
            transactions=account_data['transactions']
        )
        sync_statistics.total_transactions += len(account_data['transactions'])
    print(f"✅ Synced account: {account_data['name']} with {sync_statistics.total_transactions} transactions")
    return sync_statistics


def sync_all_accounts(api_response: dict) -> None:
    """
    Sync all accounts from SimpleFin API response
    
    Args:
        api_response: Full API response with 'accounts' list
    """
    for account in api_response.get('accounts', []):
        sync_account_from_api(account)

    print(f"✅ Synced {len(api_response.get('accounts', []))} accounts")

sync_all_accounts(data)