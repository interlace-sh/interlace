-- Migration: Initial schema setup
-- This migration creates initial tables if needed

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON public.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_product_id ON public.orders(product_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON public.orders(order_date);

