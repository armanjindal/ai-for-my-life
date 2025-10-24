from dataclasses import dataclass
from pydantic_ai import Agent, RunContext
from dotenv import load_dotenv

# Import your database functions
from src.services.db import get_todays_transactions 

load_dotenv()

@dataclass
class FinanceAgentDeps:
    """
    This is the dependency container - think of it as a backpack!
    It holds everything your agent needs to access during its run.
    """
    todays_transactions: list[dict]  # Today's transactions from DB
    financial_goals: list[str]  # Financial goals from DB


# Step 2: Create the agent and tell it what type of dependencies to expect
finance_agent = Agent(
    model='anthropic:claude-3-5-haiku-20241022',
    deps_type=FinanceAgentDeps,
    output_type=str,
)


@finance_agent.system_prompt
def get_system_prompt(ctx: RunContext[FinanceAgentDeps]) -> str:
    """
    The agent calls this to build its system prompt.
    ctx.deps gives access to the "backpack" (FinanceAgentDeps)
    """
    transactions = ctx.deps.todays_transactions
    goals = ctx.deps.financial_goals
    
    txn_summary = "\n".join([
        f"- ${txn['amount']:.2f} at {txn['payee']} ({txn['description']})"
        for txn in transactions
    ])
    
    goals_summary = "\n".join([f"- {goal}" for goal in goals])
    
    return f"""You are a finance agent helping me achieve my financial goals.

My Financial Goals:
{goals_summary}

Today's Transactions ({len(transactions)} total):
{txn_summary}

Help me understand my spending and make decisions to achieve my goals."""


# Step 4: Example of how to use the agent
def main():
    """
    Example usage - this shows how to run the agent with dependencies
    """
    # Fetch today's transactions from database using SQL query
    todays_transactions = get_todays_transactions()
    
    # Create your "backpack" with the data
    deps = FinanceAgentDeps(
        todays_transactions=todays_transactions,
        financial_goals=[
            "Spend less than $50/day on average",
            "Reduce dining out expenses",
            "Track all subscriptions"
        ]
    )
    
    result = finance_agent.run_sync(
        'How am I doing today with my spending?',
        deps=deps
    )
    
    print(result.output)


if __name__ == "__main__":
    main()
