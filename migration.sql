-- ============================================================================
-- SQL Migration Script for Hotel Ordering System (Supabase)
-- Run this in the Supabase SQL Editor to verify/create missing tables for Inventory
-- ============================================================================

-- 1. Create Locations Table
CREATE TABLE IF NOT EXISTS inventory_locations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Seed Locations (1=Godown, 2=Shop)
INSERT INTO inventory_locations (id, name) VALUES (1, 'Godown') ON CONFLICT (id) DO NOTHING;
INSERT INTO inventory_locations (id, name) VALUES (2, 'Shop') ON CONFLICT (id) DO NOTHING;

-- 2. Create Ledger Table (The core of IMS)
CREATE TABLE IF NOT EXISTS inventory_ledger (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    location_id INTEGER NOT NULL REFERENCES inventory_locations(id),
    transaction_type TEXT NOT NULL, -- RECEIVE, TRANSFER_IN, TRANSFER_OUT, SALE, ADJUSTMENT
    qty_change REAL NOT NULL,
    qty_in REAL DEFAULT 0,
    qty_out REAL DEFAULT 0,
    running_balance REAL, 
    reference_type TEXT, -- PO, ORDER, ADJUSTMENT, TRANSFER
    reference_id TEXT, -- PO#123, Order#456, "Damaged"
    remark TEXT,
    created_by TEXT DEFAULT 'system',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_ledger_product ON inventory_ledger(product_id);
CREATE INDEX IF NOT EXISTS idx_ledger_location ON inventory_ledger(location_id);
CREATE INDEX IF NOT EXISTS idx_ledger_created_at ON inventory_ledger(created_at);

-- 3. Add Reorder Leves to Products Table (if not exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='products' AND column_name='reorder_level_shop') THEN
        ALTER TABLE products ADD COLUMN reorder_level_shop INTEGER DEFAULT 5;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='products' AND column_name='reorder_level_godown') THEN
        ALTER TABLE products ADD COLUMN reorder_level_godown INTEGER DEFAULT 10;
    END IF;

    -- Safety check for inventory_ledger.remark (if table existed prior)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='inventory_ledger' AND column_name='remark') THEN
        ALTER TABLE inventory_ledger ADD COLUMN remark TEXT;
    END IF;
END $$;

-- 4. Enable RLS (Optional but recommended, though our app uses Service Role usually)
ALTER TABLE inventory_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE inventory_locations ENABLE ROW LEVEL SECURITY;

-- Allow public access for now if using anon key, or ensure Service Role bypasses this.
-- For simple internal apps, you might leave specific policies.
-- Creating a policy allowing all access for simplicity in this migration:
CREATE POLICY "Allow All" ON inventory_ledger FOR ALL USING (true);
CREATE POLICY "Allow All" ON inventory_locations FOR ALL USING (true);

-- 5. Customer Categories
CREATE TABLE IF NOT EXISTS customer_categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    parent_id INTEGER DEFAULT NULL,
    delivery_days TEXT, -- JSON list like ["MONDAY", "WEDNESDAY"]
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 6. Add constraints for parent_id (self-referencing) if desired, but optional for MVP.

-- Safety check: Add delivery_days and parent_id if missing (for iterative updates)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='customer_categories' AND column_name='delivery_days') THEN
        ALTER TABLE customer_categories ADD COLUMN delivery_days TEXT;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='customer_categories' AND column_name='parent_id') THEN
        ALTER TABLE customer_categories ADD COLUMN parent_id INTEGER DEFAULT NULL;
    END IF;
END $$;
