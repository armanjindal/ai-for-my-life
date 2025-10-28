import sys
from src.services.db import get_db

def check_database():
    """Verify database connection"""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return True
    except Exception as e:
        print(f"Database check failed: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    if check_database():
        print("✅ Health check passed")
        sys.exit(0)
    else:
        print("❌ Health check failed")
        sys.exit(1)