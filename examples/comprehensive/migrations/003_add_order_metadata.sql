-- Migration: Add order metadata columns
-- This demonstrates adding multiple columns via migration

-- Add metadata columns to orders table
ALTER TABLE public.orders ADD COLUMN IF NOT EXISTS shipping_method VARCHAR;
ALTER TABLE public.orders ADD COLUMN IF NOT EXISTS discount_amount DECIMAL(10, 2) DEFAULT 0.0;
ALTER TABLE public.orders ADD COLUMN IF NOT EXISTS notes TEXT;

