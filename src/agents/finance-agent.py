from dataclasses import dataclass
from pydantic_ai import Agent, RunContext
from dotenv import load_dotenv
import logfire
from pydantic_core import to_jsonable_python

from src.services.db import (
    get_transactions_for_period,
    create_chat_session, 
    save_messages, 
    load_messages,
    get_user_sessions,
    create_user
)


load_dotenv()
logfire.instrument_pydantic_ai()
logfire.configure()


@dataclass
class FinanceAgentDeps:
    """
    This is the dependency container - think of it as a backpack!
    It holds everything your agent needs to access during its run.
    """
    financial_goals: list[str]  # Financial goals from DB


finance_agent = Agent(
    model='anthropic:claude-3-5-haiku-20241022',
    deps_type=FinanceAgentDeps,
    output_type=str,
)


@finance_agent.system_prompt
def get_system_prompt(ctx: RunContext[FinanceAgentDeps]) -> str:
    """
    The agent calls this to build its system prompt.
    """
    goals = ctx.deps.financial_goals
    goals_summary = "\n".join([f"- {goal}" for goal in goals])
    
    return f"""You are a personal finance assistant helping me achieve my financial goals.

My Financial Goals:
{goals_summary}

You have access to tools to retrieve transaction data for any time period.
When the user asks about their spending, you should:
1. Determine the appropriate time period from their query (e.g., "today" = 1 day, "last two days" = 2 days, "this week" = 7 days)
2. Use the get_transactions tool to fetch the relevant transactions
3. Analyze the spending patterns against the financial goals
4. Provide clear, actionable insights and recommendations

Be conversational and helpful. Focus on helping me make better financial decisions."""


@finance_agent.tool
def get_transactions(ctx: RunContext[FinanceAgentDeps], days: int) -> str:
    """
    Retrieve transactions for the last N days.
    
    Args:
        days: Number of days to look back (e.g., 1 for today, 2 for last two days, 7 for last week, 30 for last month)
        
    Returns:
        A formatted summary of transactions including amounts, merchants, and descriptions
    """
    transactions = get_transactions_for_period(days)
    
    if not transactions:
        return f"No transactions found in the last {days} day(s)."
    
    # Calculate summary statistics
    total_spent = sum(txn['amount'] for txn in transactions)
    avg_per_day = total_spent / days if days > 0 else 0
    
    # Group transactions by category/merchant
    txn_details = []
    for txn in transactions:
        date_str = txn['transacted_at'][:10] if isinstance(txn['transacted_at'], str) else str(txn['transacted_at'])
        pending_str = " (PENDING)" if txn.get('pending') else ""
        txn_details.append(
            f"  • {date_str}: ${txn['amount']:.2f} at {txn['payee']} - {txn['description']}{pending_str}"
        )
    
    result = f"""Transaction Summary for Last {days} Day(s):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 Statistics:
  • Total Transactions: {len(transactions)}
  • Total Amount: ${total_spent:.2f}
  • Average per Day: ${avg_per_day:.2f}

💳 Transaction Details:
{chr(10).join(txn_details)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    return result


def main():
    """
    Multi-turn conversation with database persistence
    """
    # Hard-coded Arman User ID
    user_id = "496a7de4-4e10-4a92-93be-248d176220cd"
    
    # Create dependencies
    deps = FinanceAgentDeps(
        financial_goals=[
            "Spend less than $50/day on average",
            "Reduce dining out expenses",
            "Track all subscriptions"
        ]
    )
    
    # resume from an existing chat session
    session_id = '9a3aa386-4b35-491c-b5ca-066541032c76'
    print(f"✅ Resumed chat session: {session_id}")
    print("=" * 60)
    
    # Load conversation history (empty for new session)
    conversation_history = load_messages(session_id)
    print(f"📚 Loaded {len(conversation_history)} messages from history\n")
    
    print("Finance Agent - Multi-Turn Chat (with Database)")
    print("Type 'quit' to exit, 'sessions' to list all sessions\n")
    
    while True:
        user_query = input("You: ").strip()
        
        # Handle commands
        if user_query.lower() in ['quit', 'exit', 'q']:
            print(f"\n💾 Session saved: {session_id}")
            print("👋 Goodbye!")
            break
        
        if user_query.lower() == 'sessions':
            sessions = get_user_sessions(user_id, limit=10)
            print("\n📋 Your recent sessions:")
            for sess in sessions:
                print(f"  • {sess['title']} ({sess['message_count']} msgs) - {sess['updated_at']}")
            print()
            continue
        
        if not user_query:
            continue
        
        # Run the agent with conversation history
        result = finance_agent.run_sync(
            user_query, 
            deps=deps,
            message_history=conversation_history
        )
        
        # Display response
        print(f"\n🤖 Agent: {result.output}\n")
        print("-" * 60)
        
        # Save new messages to database
        new_messages = result.new_messages()
        save_messages(session_id, new_messages)
        
        # Update in-memory history
        conversation_history.extend(new_messages)




if __name__ == "__main__":
    main()
