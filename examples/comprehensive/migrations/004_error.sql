-- Migration: Add order metadata columns
-- This demonstrates adding multiple columns via migration

-- Add metadata columns to orders table
ALTER TABLE public.error_table ADD COLUMN IF NOT EXISTS error_column VARCHAR;