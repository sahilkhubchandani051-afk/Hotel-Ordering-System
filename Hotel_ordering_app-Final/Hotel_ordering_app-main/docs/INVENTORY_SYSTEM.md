# Inventory Management System (IMS) v2

## Overview
A Multi-Location Inventory System designed for B2B operations. It separates stock into **GODOWN** (Warehouse) and **SHOP** (Sales Floor).

## Core Concepts
1.  **Strict Partitioning**:
    *   **Godown**: Storage location. Entries via Bulk Purchase or Opening Stock.
    *   **Shop**: Sales location. Entries via Stock Transfer from Godown.
2.  **Sales Rules**:
    *   Sales (Invoices, Direct Sales) **ONLY** deduct from **SHOP**.
    *   Godown stock is NEVER touched by sales.
3.  **Data Integrity**:
    *   Ledger-based (Append Only).
    *   No direct updates to stock counts; everything is a transaction.

## Workflows

### 1. Inward Stock (Opening & Purchase)
*   **Nav**: Admin > Inventory > Bulk Opening / Purchase Stock
*   **Opening Stock**: Select "Opening Stock". Tags as `OPENING_STOCK`. Hidden date/note fields.
*   **Purchase Stock**: Select "Stock Purchase". Tags as `purchase_stock`.
    *   **Fields**: Stock Date (optional), Reference Note (optional).
    *   **Use Case**: Regular stock replenishment without full invoices.
*   **Action**: Select products, set Location, enter Qty.
*   **Result**: Increases stock. Ledger shows specified date and note.

### 2. Stock Transfer
*   **Nav**: Admin > Inventory > Transfer
*   **Purpose**: Move stock from Godown to Shop (Shop is the only sellable location).
*   **Fields**: Transfer Date (optional), Reference Note.
*   **Validation**: Cannot transfer more than available Godown stock.
*   **Action**: Select products, enter Qty to move.
*   **Result**: 
    - Godown stock decreases.
    - Shop stock increases.
    - Ledger shows linked "TRANSFER_OUT" and "TRANSFER_IN" entries.

### 3. Sales (Stock Out)
*   **Trigger**: Finalizing an Invoice (via PR flow or Direct Sale confirmation).
*   **Location**: Always decreases **SHOP** stock.
*   **Transaction Type**: `SALE_OUT`.
*   **Remark**: "Auto stock-out from sale".
*   **Result**: Automatically reduces SHOP stock by the sold amount. Does not block sales if stock is insufficient (allows negative stock).

### 5. Action Intelligence
*   **Purpose**: Helps admin quickly identify and resolve stock issues.
*   **Filter**: "Needs Attention" button on Dashboard filters for Low/Out items.
*   **Actions**:
    *   **TRANSFER**: Shop is Low, Godown has stock. (Shows "Transfer Now" button).
    *   **PURCHASE**: Shop is Low, Godown is empty. (Shows "Purchase Required" badge).
    *   **MONITOR**: Shop OK, Godown Low. (Shows "Monitor Godown" badge).
*   **Quick Modal**: Transfer transfers stock immediately from modal without page reload.ock.

### 6. Stock Movement Ledger
*   **Nav**: Click on any product card in the Dashboard.
*   **Purpose**: Complete audit trail of every stock change.
*   **Filters**:
    *   **Location**: Filter by Shop / Godown / All.
    *   **Date Range**: View history for specific periods.
*   **Details**: Shows Date, Type (Sale, Transfer, Purchase), Location, Qty Change (+/-), and Running Balance.
*   **Robustness**: Handles session expiry gracefully by redirecting to login instead of crashing.

## Database Schema (v2)
*   `inventory_locations`: 1=Godown, 2=Shop.
*   `inventory_ledger`: The single source of truth. Contains `location_id`.
*   `product_inventory_settings`: Stores default location preference per product.
*   `view_inventory_summary`: Live view aggregating ledger entries.

## Technical Notes
*   **Service**: `InventoryService` handles all logic including self-healing running balances.
*   **API**: `/admin/inventory/*` endpoints drive the UI.
*   **Legacy Compat**: Does not modify core `products` or `invoices` tables.
