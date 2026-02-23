import logging
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict
try:
    import psycopg2
    from psycopg2 import extras as psycopg2_extras
except ImportError:
    psycopg2 = None  # type: ignore
    psycopg2_extras = None  # type: ignore


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InventoryService:
    def __init__(self, db_connection):
        self.conn = db_connection
        self.is_postgres = False
        
        # Detect if we have a raw connection or a wrapper or a specific type
        if hasattr(self.conn, 'conn'):
            # Our custom PostgreSQLWrapper
            self.conn = self.conn.conn
            self.is_postgres = True
        elif isinstance(self.conn, sqlite3.Connection):
            self.is_postgres = False
        else:
            # Assume it's a raw Pg connection if not sqlite
            self.is_postgres = True

    def get_cursor(self):
        if self.is_postgres:
            return self.conn.cursor(cursor_factory=psycopg2_extras.DictCursor)
        else:
            return self.conn.cursor()


    # -------------------------------------------------------------------------
    # Helper to calculate running balance
    # -------------------------------------------------------------------------
    def _calculate_running_balance(self, cursor, product_id: int, location_id: int) -> float:
        """
        Calculates the current balance for a product at a location by summing the ledger.
        This is safer than a snapshot column for v2 design.
        """
        query = """
            SELECT SUM(qty_change) 
            FROM inventory_ledger 
            WHERE product_id = %s AND location_id = %s
        """ if self.is_postgres else """
            SELECT SUM(qty_change) 
            FROM inventory_ledger 
            WHERE product_id = ? AND location_id = ?
        """
        cursor.execute(query, (product_id, location_id))
        row = cursor.fetchone()
        return float(row[0] or 0) if row else 0.0

    def is_ledger_empty(self, product_id: int, location_id: int) -> bool:
        """
        Checks if a product has NO history at a specific location.
        Used for validating Opening Stock entries.
        """
        cursor = self.get_cursor()
        query = "SELECT 1 FROM inventory_ledger WHERE product_id = %s AND location_id = %s LIMIT 1" if self.is_postgres else \
                "SELECT 1 FROM inventory_ledger WHERE product_id = ? AND location_id = ? LIMIT 1"
        
        cursor.execute(query, (product_id, location_id))
        return cursor.fetchone() is None

    # -------------------------------------------------------------------------
    # CORE TRANSACTIONS
    # -------------------------------------------------------------------------

    def receive_stock(self, product_id: int, qty: float, location_id: int, reference_type: str, reference_id: str, transaction_type: str = 'RECEIVE', created_at: Optional[str] = None):
        """
        Receives stock into a specific location (GODOWN or SHOP).
        """
        try:
            cursor = self.get_cursor()
            
            # 1. Insert Ledger Entry
            if self.is_postgres:
                if created_at:
                     cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, reference_type, reference_id, created_by, created_at)
                        VALUES (%s, %s, %s, %s, %s, 0, %s, %s, 'system', %s)
                    """, (product_id, location_id, transaction_type, qty, qty, reference_type, reference_id, created_at))
                else:
                    cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, reference_type, reference_id, created_by)
                        VALUES (%s, %s, %s, %s, %s, 0, %s, %s, 'system')
                    """, (product_id, location_id, transaction_type, qty, qty, reference_type, reference_id))
            else:
                if created_at:
                    cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, reference_type, reference_id, created_by, created_at)
                        VALUES (?, ?, ?, ?, ?, 0, ?, ?, 'system', ?)
                    """, (product_id, location_id, transaction_type, qty, qty, reference_type, reference_id, created_at))
                else:
                    cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, reference_type, reference_id, created_by)
                        VALUES (?, ?, ?, ?, ?, 0, ?, ?, 'system')
                    """, (product_id, location_id, transaction_type, qty, qty, reference_type, reference_id))

            # 2. Update Running Balance (Self-Healing)
            # We fetch the new sum and update the just-inserted row to freeze the running balance
            new_balance = self._calculate_running_balance(cursor, product_id, location_id)
            
            # Update the last row's running_balance
            # Note: This relies on being in the same transaction context
            # In Postgres `lastval()` or `RETURNING` is better, but sqlite `lastrowid` works
            last_id = cursor.lastrowid
            
            if self.is_postgres:
                # For Pg, we need to get the ID if we didn't use RETURNING
                pass # TODO: Optimize for Pg later if needed, assume sequence logic holds
            else:
                cursor.execute("UPDATE inventory_ledger SET running_balance = ? WHERE id = ?", (new_balance, last_id))

            self.conn.commit()
            return {"success": True, "new_stock": new_balance}
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error calling receive_stock: {e}")
            raise e
        finally:
            if cursor:
                cursor.close()

    def reduce_stock(self, product_id: int, qty: float, reference_type: str, reference_id: str, remark: Optional[str] = None, allow_negative: bool = True, transaction_type: str = 'SALE', commit: bool = True):
        """
        Reduces stock from SHOP (Location ID 2) ONLY.
        """
        SHOP_LOCATION_ID = 2
        try:
            cursor = self.get_cursor()
            
            # Check Balance
            current_balance = self._calculate_running_balance(cursor, product_id, SHOP_LOCATION_ID)
            new_balance = current_balance - qty
            
            if not allow_negative and new_balance < 0:
                 raise ValueError(f"Insufficient stock in SHOP for product {product_id}. Available: {current_balance}, Needed: {qty}")

            # Insert Ledger Entry
            if self.is_postgres:
                cursor.execute("""
                    INSERT INTO inventory_ledger 
                    (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id, remark, created_by)
                    VALUES (%s, %s, %s, %s, 0, %s, %s, %s, %s, %s, 'system')
                """, (product_id, SHOP_LOCATION_ID, transaction_type, -qty, qty, new_balance, reference_type, reference_id, remark))
            else:
                cursor.execute("""
                    INSERT INTO inventory_ledger 
                    (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id, remark, created_by)
                    VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, 'system')
                """, (product_id, SHOP_LOCATION_ID, transaction_type, -qty, qty, new_balance, reference_type, reference_id, remark))
                
            if commit:
                self.conn.commit()
            return {"success": True, "new_stock": new_balance}
            
        except Exception as e:
            if commit:
                self.conn.rollback()
            logger.error(f"Error calling reduce_stock: {e}")
            raise e
        finally:
            if cursor:
                cursor.close()

    def get_product_shop_stock(self, product_id: int) -> float:
        """
        Helper to get current SHOP stock for a product.
        """
        SHOP_LOCATION_ID = 2
        cursor = self.get_cursor()
        try:
            return self._calculate_running_balance(cursor, product_id, SHOP_LOCATION_ID)
        finally:
            cursor.close()

    def record_sale_out(self, product_id: int, qty: float, reference_type: str, reference_id: str, commit: bool = True):
        """
        Wrapper specifically for Sales-Driven Auto Stock Out.
        Deducts from SHOP with SALE type.
        """
        return self.reduce_stock(
            product_id=product_id,
            qty=qty,
            reference_type=reference_type,
            reference_id=reference_id,
            remark="Auto stock-out from sale",
            transaction_type='SALE',
            commit=commit
        )

    def transfer_stock(self, product_id: int, qty: float, from_loc: int, to_loc: int, reference_id: str, created_at: Optional[str] = None):
        """
        Transfers stock between locations.
        Double entry: Out from Source, In to Dest.
        """
        try:
            cursor = self.get_cursor()
            
            # Check Source Balance
            source_bal = self._calculate_running_balance(cursor, product_id, from_loc)
            if source_bal < qty:
                raise ValueError(f"Insufficient stock in Source Location to transfer {qty}. Available: {source_bal}")

            # 1. OUT from Source
            new_source_bal = source_bal - qty
            if self.is_postgres:
                if created_at:
                    cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id, created_at)
                        VALUES (%s, %s, 'TRANSFER_OUT', %s, 0, %s, %s, 'TRANSFER', %s, %s)
                    """, (product_id, from_loc, -qty, qty, new_source_bal, reference_id, created_at))
                else:
                    cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id)
                        VALUES (%s, %s, 'TRANSFER_OUT', %s, 0, %s, %s, 'TRANSFER', %s)
                    """, (product_id, from_loc, -qty, qty, new_source_bal, reference_id))
            else:
                if created_at:
                    cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id, created_at)
                        VALUES (?, ?, 'TRANSFER_OUT', ?, 0, ?, ?, 'TRANSFER', ?, ?)
                    """, (product_id, from_loc, -qty, qty, new_source_bal, reference_id, created_at))
                else:
                    cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id)
                        VALUES (?, ?, 'TRANSFER_OUT', ?, 0, ?, ?, 'TRANSFER', ?)
                    """, (product_id, from_loc, -qty, qty, new_source_bal, reference_id))
                
            # 2. IN to Dest
            dest_bal = self._calculate_running_balance(cursor, product_id, to_loc)
            new_dest_bal = dest_bal + qty
            
            if self.is_postgres:
                if created_at:
                    cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id, created_at)
                        VALUES (%s, %s, 'TRANSFER_IN', %s, %s, 0, %s, 'TRANSFER', %s, %s)
                    """, (product_id, to_loc, qty, qty, new_dest_bal, reference_id, created_at))
                else:
                    cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id)
                        VALUES (%s, %s, 'TRANSFER_IN', %s, %s, 0, %s, 'TRANSFER', %s)
                    """, (product_id, to_loc, qty, qty, new_dest_bal, reference_id))
            else:
                if created_at:
                    cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id, created_at)
                        VALUES (?, ?, 'TRANSFER_IN', ?, ?, 0, ?, 'TRANSFER', ?, ?)
                    """, (product_id, to_loc, qty, qty, new_dest_bal, reference_id, created_at))
                else:
                    cursor.execute("""
                        INSERT INTO inventory_ledger 
                        (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id)
                        VALUES (?, ?, 'TRANSFER_IN', ?, ?, 0, ?, 'TRANSFER', ?)
                    """, (product_id, to_loc, qty, qty, new_dest_bal, reference_id))
                
            self.conn.commit()
            return {"success": True}
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error calling transfer_stock: {e}")
            raise e
        finally:
            if cursor:
                cursor.close()

    def get_stock_summary(self):
        """
        Queries the view_inventory_summary to get current stocks.
        Returns flattened list: [{product_id, name, godown_stock, shop_stock}, ...]
        """
        try:
            cursor = self.get_cursor()
            # Pivot the view logic here for easier consumption
            query = """
                SELECT 
                    p.id, p.name, p.unit, p.reorder_level_shop, p.reorder_level_godown,
                    SUM(CASE WHEN l.location_id = 1 THEN l.qty_change ELSE 0 END) as godown_stock,
                    SUM(CASE WHEN l.location_id = 2 THEN l.qty_change ELSE 0 END) as shop_stock
                FROM products p
                LEFT JOIN inventory_ledger l ON p.id = l.product_id
                GROUP BY p.id, p.name
                ORDER BY p.name
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
        finally:
            if cursor:
                cursor.close()

    def get_ledger(self, product_id: int, start_date: str = None, end_date: str = None, location_id: int = None, transaction_type: str = None):
        try:
            cursor = self.get_cursor()
            
            # Base Query
            query = """
                SELECT l.*, loc.name as location_name 
                FROM inventory_ledger l
                LEFT JOIN inventory_locations loc ON l.location_id = loc.id
                WHERE l.product_id = param_pid 
            """
            
            params = {'param_pid': product_id}
            
            # Filters
            if start_date:
                query += " AND l.created_at >= param_start"
                params['param_start'] = start_date
            if end_date:
                 # Add one day to include the end date fully if it's just YYYY-MM-DD
                query += " AND l.created_at <= param_end"
                params['param_end'] = end_date + " 23:59:59"
            if location_id:
                query += " AND l.location_id = param_loc"
                params['param_loc'] = location_id
            if transaction_type:
                 query += " AND l.transaction_type = param_type"
                 params['param_type'] = transaction_type

            query += " ORDER BY l.created_at DESC"
            
            # Param substitution
            if self.is_postgres:
                # Replace named params with %s and build tuple
                # This is tricky with raw SQL string manipulation + dict params. 
                # Simpler to just build list of args and use %s or ?
                
                final_query = query.replace('param_pid', '%s').replace('param_start', '%s').replace('param_end', '%s').replace('param_loc', '%s').replace('param_type', '%s')
                final_args = []
                final_args.append(product_id)
                if start_date: final_args.append(start_date)
                if end_date: final_args.append(end_date + " 23:59:59")
                if location_id: final_args.append(location_id)
                if transaction_type: final_args.append(transaction_type)
                
                cursor.execute(final_query, tuple(final_args))
            else:
                # SQLite ?
                final_query = query.replace('param_pid', '?').replace('param_start', '?').replace('param_end', '?').replace('param_loc', '?').replace('param_type', '?')
                final_args = []
                final_args.append(product_id)
                if start_date: final_args.append(start_date)
                if end_date: final_args.append(end_date + " 23:59:59")
                if location_id: final_args.append(location_id)
                if transaction_type: final_args.append(transaction_type)
                
                cursor.execute(final_query, tuple(final_args))

            rows = cursor.fetchall()
            return [dict(r) for r in rows]
        finally:
            if cursor:
                cursor.close()

    def adjust_stock(self, product_id: int, location_id: int, qty: float, adjustment_type: str, reason: str, remark: Optional[str] = None):
        """
        Adjusts stock for a specific location (Increase or Decrease).
        Creates a new transaction record. Never modifies history.
        
        adjustment_type: 'INCREASE' or 'DECREASE'
        qty: Absolute quantity value (must be > 0)
        """
        if qty <= 0:
            raise ValueError("Adjustment quantity must be positive")
            
        try:
            cursor = self.get_cursor()
            
            # Calculate final signed change
            qty_change = qty if adjustment_type == 'INCREASE' else -qty
            qty_in = qty if adjustment_type == 'INCREASE' else 0
            qty_out = qty if adjustment_type == 'DECREASE' else 0
            
            # Get Current Balance
            current_balance = self._calculate_running_balance(cursor, product_id, location_id)
            new_balance = current_balance + qty_change
            
            # Validate Negative Stock logic (Optional: allow negative for adjustments to fix errors, or block?)
            # Usually adjustments are TO fix negative stock, or might cause it if correcting a bad count.
            # Let's allow negative for ADJUSTMENT to ensure flexibility in fixing data messes.
            
            full_remark = f"{reason}"
            if remark:
                full_remark += f" - {remark}"

            # Insert Ledger Entry
            if self.is_postgres:
                cursor.execute("""
                    INSERT INTO inventory_ledger 
                    (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id, remark, created_by)
                    VALUES (%s, %s, 'ADJUSTMENT', %s, %s, %s, %s, 'ADJUSTMENT', %s, %s, 'system')
                """, (product_id, location_id, qty_change, qty_in, qty_out, new_balance, reason, full_remark))
            else:
                cursor.execute("""
                    INSERT INTO inventory_ledger 
                    (product_id, location_id, transaction_type, qty_change, qty_in, qty_out, running_balance, reference_type, reference_id, remark, created_by)
                    VALUES (?, ?, 'ADJUSTMENT', ?, ?, ?, ?, 'ADJUSTMENT', ?, ?, 'system')
                """, (product_id, location_id, qty_change, qty_in, qty_out, new_balance, reason, full_remark))

            # Trigger immediate re-calculation update (for sqlite primarily)
            # PG relies on insert order or we can update `running_balance` if we change logic slightly, 
            # but standard ledger means we just insert. 
            # However, `receive_stock` updates the row for running_balance persistence in SQLite logic. 
            # In V2, we are storing `running_balance` in the ledger row for faster reads.
            
            # For robustness, we just make sure the INSERT included the calculated `reading_balance`.
            # The calculation `self._calculate_running_balance` sums ALL history. 
            # Since we just inserted a new row, the `current_balance` (from sum) EXCLUDED the new row.
            # So `new_balance = current_balance + qty_change` is CORRECT.
            # The specific logic in `receive_stock` that updates running_balance post-insert is possibly redundant 
            # if we trust the python calc, but safer if we want DB-side truth.
            # Here we stick to Python calc + Insert for simplicity and speed.

            self.conn.commit()
            return {"success": True, "new_stock": new_balance}
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error calling adjust_stock: {e}")
            raise e
        finally:
            if cursor:
                cursor.close()
