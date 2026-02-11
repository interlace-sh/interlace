-- Migration: Add customer segment column
-- This demonstrates schema evolution via migration

-- Add customer_segment column to customers table
ALTER TABLE public.customers ADD COLUMN IF NOT EXISTS customer_segment VARCHAR;

-- Update existing customers with default segment
UPDATE public.customers 
SET customer_segment = 'standard' 
WHERE customer_segment IS NULL;

