-- Add loyalty_tier column to customers table
ALTER TABLE customers ADD COLUMN IF NOT EXISTS loyalty_tier VARCHAR DEFAULT 'bronze';
