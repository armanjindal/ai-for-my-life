import psycopg2 as psycopg

from contextlib import contextmanager
from dotenv import load_dotenv
import os
import logfire

load_dotenv()
logfire.configure()

DATABASE_URL = os.getenv('DATABASE_URL')

@contextmanager
def get_db():
    conn = psycopg.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def test_connection():
    with get_db() as conn:
        with conn.cursor() as cur:
            
            cur.execute("SELECT * FROM accounts")
            accounts = cur.fetchall()
            for account in accounts:
                print(account)

test_connection()

def update_accounts_table(account_id: str, name: str, currency: str, balance: float, available_balance: float, balance_date: int)->int:
    """
    Update an account in the database
    """
    with logfire.span('db.update_accounts_table', account_id=account_id, name=name, currency=currency):
        logfire.info('Updating account', 
                     account_id=account_id, 
                     name=name, 
                     balance=balance, 
                     available_balance=available_balance)
        
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO accounts (id, name, currency, balance, available_balance, balance_date)
                        VALUES (%s, %s, %s, %s, %s, to_timestamp(%s))
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            balance = EXCLUDED.balance,
                            available_balance = EXCLUDED.available_balance,
                            balance_date = EXCLUDED.balance_date,
                            updated_at = NOW();
                    """, (account_id, name, currency, balance, available_balance, balance_date))
                    conn.commit()
                    row_count = cur.rowcount
                    
                    logfire.info('Account update complete', 
                                 account_id=account_id, 
                                 rows_affected=row_count,
                                 operation='insert' if row_count == 1 else 'update')
                    return row_count
        except Exception as e:
            logfire.error('Failed to update account', 
                          account_id=account_id, 
                          error=str(e))
            raise

def update_transactions_table(account_id: str, transactions: list[dict]) -> int:
    """
    Insert or update transactions for an account
    
    Args:
        account_id: The account ID these transactions belong to
        transactions: List of transaction dicts from SimpleFin API
        
    Returns:
        Number of transactions inserted/updated
    """
    with logfire.span('db.update_transactions_table', 
                      account_id=account_id, 
                      transaction_count=len(transactions)):
        logfire.info('Updating transactions', 
                     account_id=account_id, 
                     transaction_count=len(transactions))
        
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    count = 0
                    total_amount = 0.0
                    pending_count = 0
                    
                    for txn in transactions:
                        amount = float(txn['amount'])
                        is_pending = txn.get('pending', False)
                        logfire.info(f'Inserting transaction ${txn["id"]}')
                        cur.execute("""
                            INSERT INTO transactions (
                                id, account_id, posted, amount, description, 
                                payee, memo, transacted_at, pending
                            )
                            VALUES (
                                %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s), %s
                            )
                            ON CONFLICT (id) DO UPDATE SET
                                posted = EXCLUDED.posted,
                                amount = EXCLUDED.amount,
                                description = EXCLUDED.description,
                                payee = EXCLUDED.payee,
                                memo = EXCLUDED.memo,
                                transacted_at = EXCLUDED.transacted_at,
                                pending = EXCLUDED.pending,
                                last_updated_at = NOW()
                        """, (
                            txn['id'],
                            account_id,
                            txn['posted'] == 1,  # Convert 0/1 to boolean
                            amount,
                            txn.get('description', ''),
                            txn.get('payee', ''),
                            txn.get('memo', ''),
                            txn['transacted_at'],
                            is_pending
                        ))
                        count += cur.rowcount
                        total_amount += amount
                        if is_pending:
                            pending_count += 1
                    
                    logfire.info('Transactions update complete', 
                                 account_id=account_id,
                                 rows_affected=count,
                                 total_amount=total_amount,
                                 pending_count=pending_count,
                                 processed_count=len(transactions))
                    return count
        except Exception as e:
            logfire.error('Failed to update transactions', 
                          account_id=account_id,
                          transaction_count=len(transactions),
                          error=str(e))
            raise

def update_account_snapshots_table(account_id: str, balance: float, available_balance: float, balance_date: int) -> int:
    """
    Insert a snapshot of an account's balance
    
    Args:
        account_id: The account ID
        balance: Current balance
        available_balance: Available balance
        balance_date: Unix timestamp of when this balance was recorded
        
    Returns:
        Number of rows inserted (0 if snapshot already exists for this date, 1 if new)
    """
    with logfire.span('db.update_account_snapshots_table', account_id=account_id):
        logfire.info('Updating account snapshot', 
                     account_id=account_id, 
                     balance=balance, 
                     available_balance=available_balance,
                     balance_date=balance_date)
        
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO account_snapshots (
                            account_id, balance, available_balance, balance_date
                        )
                        VALUES (%s, %s, %s, to_timestamp(%s))
                        ON CONFLICT (account_id, balance_date) DO UPDATE SET
                            balance = EXCLUDED.balance,
                            available_balance = EXCLUDED.available_balance,
                            snapshot_taken_at = NOW()
                    """, (account_id, balance, available_balance, balance_date))
                    row_count = cur.rowcount
                    
                    logfire.info('Account snapshot update complete', 
                                 account_id=account_id,
                                 rows_affected=row_count,
                                 operation='insert' if row_count == 1 else 'update',
                                 balance=balance,
                                 available_balance=available_balance)
                    return row_count
        except Exception as e:
            logfire.error('Failed to update account snapshot', 
                          account_id=account_id,
                          error=str(e))
            raise

def get_todays_transactions() -> list[dict]:
    """
    Get all transactions from today
    
    Returns:
        List of transaction dictionaries
    """
    with logfire.span('db.get_todays_transactions'):
        logfire.info('Fetching today\'s transactions')
        
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            a.name as account_name,
                            t.amount,
                            t.description,
                            t.payee,
                            t.memo,
                            t.transacted_at,
                            t.pending
                        FROM transactions t
                        JOIN accounts a ON t.account_id = a.id
                        WHERE DATE(t.transacted_at) = CURRENT_DATE - INTERVAL '1 day'
                        ORDER BY t.transacted_at DESC
                    """)
                
                    columns = [desc[0] for desc in cur.description]
                    transactions = []
                    
                    for row in cur.fetchall():
                        txn = dict(zip(columns, row))
                        transactions.append(txn)
                    
                    logfire.info('Fetched today\'s transactions', 
                                 count=len(transactions))
                    return transactions
        except Exception as e:
            logfire.error('Failed to fetch today\'s transactions', 
                          error=str(e))
            raise