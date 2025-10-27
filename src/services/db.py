import psycopg2 as psycopg

from contextlib import contextmanager
from dotenv import load_dotenv
import os
import logfire
from pydantic_ai import ModelMessagesTypeAdapter
from pydantic_core import to_jsonable_python
import uuid
import json

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

def get_transactions_for_period(days: int) -> list[dict]:
    """
    Get transactions for the last N days
    
    Args:
        days: Number of days to look back (e.g., 1 for today, 7 for last week)
        
    Returns:
        List of transaction dictionaries with account name, amount, description, etc.
    """
    with logfire.span('db.get_transactions_for_period', days=days):
        logfire.info('Fetching transactions for period', days=days)
        
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    # Use parameterized query with interval calculation
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
                        WHERE t.transacted_at >= CURRENT_DATE - (INTERVAL '1 day' * %s)
                        ORDER BY t.transacted_at DESC
                    """, (days,))
                
                    columns = [desc[0] for desc in cur.description]
                    transactions = []
                    
                    for row in cur.fetchall():
                        txn = dict(zip(columns, row))
                        # Format the datetime as string for JSON serialization
                        if txn.get('transacted_at'):
                            txn['transacted_at'] = txn['transacted_at'].isoformat()
                        transactions.append(txn)
                    
                    logfire.info('Fetched transactions for period', 
                                 days=days,
                                 count=len(transactions))
                    return transactions
        except Exception as e:
            logfire.error('Failed to fetch transactions for period', 
                          days=days,
                          error=str(e))
            raise
    
# Saving chat history 
def create_chat_session(user_id: str, title: str = None, metadata: dict = None) -> str:
    """
    Create a new chat session
    
    Args:
        user_id: User ID who owns this session
        title: Optional session title
        metadata: Optional metadata (e.g., financial goals, agent config)
        
    Returns:
        session_id as string
    """
    with logfire.span('db.create_chat_session', user_id=user_id):
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    session_id = str(uuid.uuid4())
                    
                    cur.execute("""
                        INSERT INTO chat_sessions (session_id, user_id, title, metadata, model)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING session_id
                    """, (
                        session_id,
                        user_id,
                        title or "New Conversation",
                        json.dumps(metadata) if metadata else None,
                        "anthropic:claude-3-5-haiku-20241022"  # Default model
                    ))
                    
                    result = cur.fetchone()
                    logfire.info('Created chat session', 
                                session_id=result[0],
                                user_id=user_id)
                    return result[0]
        except Exception as e:
            logfire.error('Failed to create chat session', 
                         user_id=user_id, 
                         error=str(e))
            raise


def save_messages(session_id: str, messages: list) -> int:
    """
    Save new messages to the database
    
    Args:
        session_id: The session these messages belong to
        messages: List of ModelMessage objects from result.new_messages()
        
    Returns:
        Number of messages saved
    """
    with logfire.span('db.save_messages', session_id=session_id):
        logfire.info('Saving messages', 
                    session_id=session_id, 
                    count=len(messages))
        
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    count = 0
                    last_content = None
                    
                    for message in messages:
                        # Convert ModelMessage to JSON-serializable dict
                        message_json = to_jsonable_python(message)
                        
                        # Extract kind directly from Pydantic AI message (request or response)
                        kind = message_json.get('kind', 'request')
                        
                        # Extract text content from parts for search/display
                        content = ""
                        if 'parts' in message_json:
                            for part in message_json['parts']:
                                if isinstance(part, dict) and 'content' in part:
                                    content += str(part['content']) + " "
                                elif isinstance(part, str):
                                    content += part + " "
                        
                        content = content.strip() or "No text content"
                        last_content = content
                        
                        # Insert message with native Pydantic AI structure
                        cur.execute("""
                            INSERT INTO chat_messages (
                                session_id, 
                                kind, 
                                content, 
                                full_message_data
                            )
                            VALUES (%s, %s, %s, %s)
                        """, (
                            session_id,
                            kind,
                            content,
                            json.dumps(message_json)
                        ))
                        count += 1
                    
                    # Update session's updated_at and preview_text
                    if last_content:
                        preview = last_content[:200]  # First 200 chars
                        cur.execute("""
                            UPDATE chat_sessions 
                            SET updated_at = NOW(),
                                preview_text = %s
                            WHERE session_id = %s
                        """, (preview, session_id))
                    
                    logfire.info('Messages saved', 
                                session_id=session_id, 
                                count=count)
                    return count
        except Exception as e:
            logfire.error('Failed to save messages', 
                         session_id=session_id,
                         error=str(e))
            raise

def load_messages(session_id: str) -> list:
    """
    Load all messages for a session from the database
    
    Args:
        session_id: The session ID to load
        
    Returns:
        List of ModelMessage objects validated from the stored JSON
    """
    with logfire.span('db.load_messages', session_id=session_id):
        logfire.info('Loading messages', session_id=session_id)
        
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT full_message_data 
                        FROM chat_messages 
                        WHERE session_id = %s AND NOT deleted
                        ORDER BY created_at ASC
                    """, (session_id,))
                    
                    rows = cur.fetchall()
                    
                    # Collect all message JSONs
                    messages_json = [row[0] for row in rows]  # JSONB automatically parsed to dict
                    
                    # Validate the entire list at once using ModelMessagesTypeAdapter
                    # This adapter expects a LIST of messages, not individual messages
                    messages = ModelMessagesTypeAdapter.validate_python(messages_json)
                    
                    logfire.info('Loaded messages', 
                                session_id=session_id,
                                count=len(messages))
                    return messages
        except Exception as e:
            logfire.error('Failed to load messages', 
                         session_id=session_id,
                         error=str(e))
            raise


def get_user_sessions(user_id: str, limit: int = 20, offset: int = 0, archived: bool = False) -> list[dict]:
    """
    Get list of user's chat sessions
    
    Args:
        user_id: User ID
        limit: Max sessions to return
        offset: Pagination offset
        archived: Include archived sessions
        
    Returns:
        List of session dictionaries
    """
    with logfire.span('db.get_user_sessions', user_id=user_id):
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            cs.session_id,
                            cs.title,
                            cs.preview_text,
                            cs.model,
                            cs.created_at,
                            cs.updated_at,
                            cs.archived,
                            COUNT(cm.id) as message_count
                        FROM chat_sessions cs
                        LEFT JOIN chat_messages cm 
                            ON cs.session_id = cm.session_id AND NOT cm.deleted
                        WHERE cs.user_id = %s AND cs.archived = %s
                        GROUP BY cs.session_id
                        ORDER BY cs.updated_at DESC
                        LIMIT %s OFFSET %s
                    """, (user_id, archived, limit, offset))
                    
                    columns = [desc[0] for desc in cur.description]
                    sessions = []
                    
                    for row in cur.fetchall():
                        session = dict(zip(columns, row))
                        # Convert datetime to ISO string
                        session['created_at'] = session['created_at'].isoformat()
                        session['updated_at'] = session['updated_at'].isoformat()
                        sessions.append(session)
                    
                    logfire.info('Fetched user sessions', 
                                user_id=user_id,
                                count=len(sessions))
                    return sessions
        except Exception as e:
            logfire.error('Failed to fetch user sessions', 
                         user_id=user_id,
                         error=str(e))
            raise


def update_session_title(session_id: str, title: str) -> bool:
    """
    Update session title
    
    Args:
        session_id: Session to update
        title: New title
        
    Returns:
        True if successful
    """
    with logfire.span('db.update_session_title', session_id=session_id):
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE chat_sessions 
                        SET title = %s, updated_at = NOW()
                        WHERE session_id = %s
                    """, (title, session_id))
                    
                    return cur.rowcount > 0
        except Exception as e:
            logfire.error('Failed to update session title', error=str(e))
            raise

def create_user(name: str, email: str) -> str:
    with logfire.span('db.create_user', name=name, email=email):
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    result = cur.execute("""
                        INSERT INTO users (name, email, created_at)
                        VALUES (%s, %s, NOW())
                        RETURNING id
                    """, (name, email))
                    return result
        except Exception as e:
            logfire.error('Failed to create user', error=str(e))
            raise