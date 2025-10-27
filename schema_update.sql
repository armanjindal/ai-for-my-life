-- Migration: Change role column to kind to match Pydantic AI structure

-- Step 1: Drop the old constraint
ALTER TABLE chat_messages DROP CONSTRAINT IF EXISTS chat_messages_role_check;

-- Step 2: Rename the column
ALTER TABLE chat_messages RENAME COLUMN role TO kind;

-- Step 3: Update existing data to use Pydantic AI's kind values
UPDATE chat_messages 
SET kind = CASE 
    WHEN kind = 'user' THEN 'request'
    WHEN kind = 'system' THEN 'request' 
    WHEN kind = 'tool' THEN 
        CASE 
            WHEN full_message_data->>'kind' = 'response' THEN 'response'
            ELSE 'request'
        END
    WHEN kind = 'model' THEN 'response'
    ELSE kind
END;

-- Step 4: Add new constraint matching Pydantic AI
ALTER TABLE chat_messages 
ADD CONSTRAINT chat_messages_kind_check 
CHECK (kind IN ('request', 'response'));

-- Step 5: Update index (optional, for performance)
DROP INDEX IF EXISTS idx_messages_role;
CREATE INDEX idx_messages_kind ON chat_messages(session_id, kind);
