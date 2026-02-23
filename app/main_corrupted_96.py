
import os
import sys
import asyncio
import json
import sqlite3
try:
    import psycopg2
    from psycopg2 import extras
except ImportError:
    psycopg2 = None
    extras = None
import logging
import random
import string
import re
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict
from app import utils
from app.utils import number_to_words
try:
    # Translator import removed - using REST API via utils.py
    Translator = None
except ImportError:
    Translator = None

import shutil
from fastapi import FastAPI, Request, Form, Response, Depends, HTTPException, status, UploadFile, File, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from supabase import create_client, Client

# Load environment variables
# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# --- Supabase Initialization ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:8000")

supabase: Client | None = None

if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
    supabase = create_client(
        SUPABASE_URL,
        SUPABASE_SERVICE_ROLE_KEY
    )
else:
    print("WARNING: Supabase environment variables not configured")

# --- Translation Service ---

def validate_image_url(url):
    if not url:
        return True
    try:
        import requests
        response = requests.head(url, timeout=3)
        content_type = response.headers.get('content-type', '')
        return content_type.startswith('image/')
    except:
        return False

# --- Configuration & Path Handling ---

def get_base_path():
    """ Returns the root directory of the project (parent of 'app'). """
    # This file is in app/main.py, so parent of parent of this file is the root
    current_file_path = os.path.abspath(__file__)
    app_dir = os.path.dirname(current_file_path)
    root_dir = os.path.dirname(app_dir)
    return root_dir

def get_db_path():
    """ Returns the path for the database.
        For Render, check if /app/data exists (persistent disk)
    """
    # Check for Render persistent disk
    render_data_path = "/app/data"
    if os.path.exists(render_data_path) and os.path.isdir(render_data_path):
        return render_data_path
    
    # Fallback to project root
    return get_base_path()

ROOT_PATH = get_base_path()
DB_FOLDER = get_db_path()
DB_PATH = os.path.join(DB_FOLDER, "orders.db")
DATABASE_URL = os.getenv("DATABASE_URL")
TEMPLATES_PATH = os.path.join(ROOT_PATH, "templates")
STATIC_PATH = os.path.join(ROOT_PATH, "static")
pass

# Debugging paths and database on startup
print(f"=" * 60)
print(f"DATABASE CONFIGURATION")
print(f"=" * 60)
if DATABASE_URL:
    print(f"[OK] DATABASE_URL is SET")
    print(f"[OK] USING: PostgreSQL (Production)")
    print(f"[OK] URL starts with: {DATABASE_URL[:20]}...")
else:
    # Check if we're on Render (no persistent disk)
    if os.getenv("RENDER"):
        print(f"[WARN] RENDER detected but DATABASE_URL NOT SET")
        print(f"[OK] USING: In-Memory SQLite (NO DATA PERSISTENCE)")
        print(f"[WARN] All data will be lost on restart!")
    else:
        print(f"[INFO] DATABASE_URL is NOT SET")
        print(f"[OK] USING: SQLite (Local Development)")
        print(f"[OK] DB_PATH: {DB_PATH}")
print(f"=" * 60)

# Upload support
UPLOAD_DIR = os.path.join(DB_FOLDER, "static", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Get configuration from environment variables
SESSION_SECRET = os.getenv("SESSION_SECRET_KEY", "SUPER_SECRET_KEY_CHANGE_ME")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@hotel.com")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

app = FastAPI(title="Hotel Ordering System")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)

@app.middleware("http")
async def debug_logging_middleware(request: Request, call_next):
    # Only log to stderr in production to keep it clean and visible in Render logs
    if ENVIRONMENT == "development":
        print(f"DEBUG: {request.method} {request.url}")
    
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        import traceback
        print(f"ERROR: {request.method} {request.url} failed: {e}")
        traceback.print_exc()
        raise e

# Mount static files correctly
# Mount persistent uploads directory FIRST to catch /static/uploads/ requests
app.mount("/static/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

# Mount general static files
if os.path.exists(STATIC_PATH):
    app.mount("/static", StaticFiles(directory=STATIC_PATH), name="static")
else:
    # Fallback for some deployment structures
    os.makedirs(os.path.join(DB_FOLDER, "static"), exist_ok=True)
    app.mount("/static", StaticFiles(directory=os.path.join(DB_FOLDER, "static")), name="static")

# Verify templates directory exists
print(f"=" * 60)
print(f"TEMPLATE CONFIGURATION")
print(f"=" * 60)
print(f"ROOT_PATH: {ROOT_PATH}")
print(f"TEMPLATES_PATH: {TEMPLATES_PATH}")
print(f"Templates directory exists: {os.path.exists(TEMPLATES_PATH)}")
if os.path.exists(TEMPLATES_PATH):
    template_files = os.listdir(TEMPLATES_PATH)
    print(f"Template files found: {len(template_files)}")
    print(f"Sample files: {template_files[:5] if len(template_files) > 0 else 'NONE'}")
else:
    print(f"⚠️ WARNING: Templates directory NOT FOUND!")
print(f"=" * 60)

templates = Jinja2Templates(directory=TEMPLATES_PATH)


# --- Database Setup ---

class PostgreSQLCursorWrapper:
    def __init__(self, cursor, conn):
        self.cursor = cursor
        self.conn = conn
        self._lastrowid = None

    @property
    def lastrowid(self):
        return self._lastrowid

    def execute(self, sql, params=None):
        # Convert SQLite placeholders (?) to PostgreSQL placeholders (%s)
        if "?" in sql:
            sql = sql.replace("?", "%s")
        
        # PostgreSQL-specific case sensitivity for EXCLUDED
        if "ON CONFLICT" in sql.upper() and "excluded." in sql:
            sql = sql.replace("excluded.", "EXCLUDED.")
        
        # Append RETURNING id for INSERTs to simulate lastrowid
        is_insert = sql.strip().upper().startswith("INSERT")
        if is_insert and "RETURNING" not in sql.upper():
            # Avoid RETURNING id for tables that don't have an 'id' column
            no_id_tables = ['customers', 'settings', 'otp_codes']
            sql_upper = sql.upper()
            should_append = True
            for table in no_id_tables:
                if f"INTO {table.upper()}" in sql_upper:
                    should_append = False
                    break
            
            if should_append:
                sql = sql.rstrip().rstrip(';') + " RETURNING id"

        try:
            if params is not None:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            
            if is_insert:
                try:
                    row = self.cursor.fetchone()
                    if row:
                        self._lastrowid = row[0]
                except:
                    pass
            
            return self
        except Exception as e:
            # Check if this is a PostgreSQL transaction error
            # If the transaction is aborted, we might need a rollback
            # However, we'll let the higher level handle the decision to rollback or just log.
            # But usually, any error in PG means the transaction is aborted.
            print(f"PostgreSQL Cursor Error: {e}")
            print(f"Failed SQL: {sql}")
            
            # Simple heuristic: if we get a transaction error, we should probably ROLLBACK 
            # so subsequent commands DON'T fail with "current transaction is aborted"
            if "current transaction is aborted" in str(e).lower():
                try:
                    self.conn.rollback()
                    print("Transaction rolled back due to aborted state.")
                except:
                    pass
            raise e

    def fetchone(self):
        try:
            return self.cursor.fetchone()
        except Exception as e:
            print(f"PostgreSQL fetchone error: {e}")
            return None

    def fetchall(self):
        try:
            return self.cursor.fetchall()
        except Exception as e:
            print(f"PostgreSQL fetchall error: {e}")
            return []

    def __getattr__(self, name):
        return getattr(self.cursor, name)

class PostgreSQLWrapper:
    def __init__(self, conn):
        self.conn = conn
        
    def execute(self, sql, params=None):
        cur = self.cursor()
        return cur.execute(sql, params)

    def commit(self):
        try:
            self.conn.commit()
        except Exception as e:
            print(f"PostgreSQL commit error: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            raise e

    def close(self):
        try:
            self.conn.close()
        except:
            pass

    def cursor(self):
        try:
            # use DictCursor for sqlite.Row-like behavior
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            return PostgreSQLCursorWrapper(cur, self.conn)
        except Exception as e:
            # If cursor creation fails because transaction is aborted, try a rollback
            if "current transaction is aborted" in str(e).lower():
                try:
                    self.conn.rollback()
                    cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                    return PostgreSQLCursorWrapper(cur, self.conn)
                except:
                    pass
            raise e

def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        # PostgreSQL (Supabase / Render)
        print(f"[DB] Attempting PostgreSQL connection...")
        try:
            url = db_url
            # Fix legacy postgres:// prefix if present
            if url.startswith("postgres://"):
                url = url.replace("postgres://", "postgresql://", 1)
            
            # Add SSL mode for Supabase/Render
            if "?" in url:
                if "sslmode=" not in url:
                    url += "&sslmode=require"
            else:
                url += "?sslmode=require"
            
            conn = psycopg2.connect(url, connect_timeout=10)
            conn.autocommit = False # Ensure we use transactions
            print(f"[DB] OK PostgreSQL connected successfully")
            return PostgreSQLWrapper(conn)
        except Exception as e:
            print(f"[DB] ERROR CRITICAL ERROR connecting to PostgreSQL: {e}")
            print(f"[DB] DATABASE_URL exists but connection failed!")
            raise e
    else:
        # SQLite (Local or In-Memory for Render)
        try:
            # If on Render without DATABASE_URL, use in-memory database
            if os.getenv("RENDER"):
                print(f"[DB] WARN Using IN-MEMORY SQLite (data will be lost on restart)")
                conn = sqlite3.connect(":memory:", check_same_thread=False)
            else:
                print(f"[DB] Using file-based SQLite (local development)")
                conn = sqlite3.connect(DB_PATH)
            
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            print(f"[DB] ERROR Error connecting to SQLite: {e}")
            raise e

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    is_pg = os.getenv("DATABASE_URL") is not None
    pk_type = "SERIAL PRIMARY KEY" if is_pg else "INTEGER PRIMARY KEY AUTOINCREMENT"
    supabase_user_id_type = "UUID" if is_pg else "TEXT"

    try:
        # 1. Categories (Independent) - MUST EXIST BEFORE PRODUCTS
        c.execute(f"CREATE TABLE IF NOT EXISTS categories (id {pk_type}, name TEXT UNIQUE NOT NULL)")
        
        # 2. Products (Depends on categories)
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS products (
                id {pk_type},
                name TEXT NOT NULL,
                category_id INTEGER,
                unit TEXT NOT NULL DEFAULT 'pcs',
                rate REAL NOT NULL DEFAULT 0,
                image_path TEXT,
                name_marathi TEXT,
                image_url TEXT,
                created_at TEXT,
                FOREIGN KEY (category_id) REFERENCES categories (id)
            )
        ''')
        # 3. Customers
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS customers (
                email TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                name TEXT NOT NULL,
                phone TEXT,
                full_name TEXT,
                business_name TEXT,
                mobile TEXT,
                address TEXT,
                category TEXT,
                status TEXT DEFAULT 'Active',
                supabase_user_id {supabase_user_id_type},
                created_at TEXT
            )
        ''')
        
        # 4. Customer Categories (Updated for Delivery Routes)
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS customer_categories (
                id {pk_type},
                name TEXT UNIQUE NOT NULL,
                is_active INTEGER DEFAULT 1,
                delivery_days TEXT,
                parent_id INTEGER,
                level INTEGER DEFAULT 1,
                route_name TEXT
            )
        ''')
        
        # 5. Draft PRs (New Feature)
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS draft_prs (
                id {pk_type},
                customer_email TEXT NOT NULL,
                created_at TEXT NOT NULL,
                items_json TEXT NOT NULL
            )
        ''')

        # 6. Purchase Requisitions
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS purchase_requisitions (
                id {pk_type},
                customer_email TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PR',
                created_at TEXT NOT NULL,
                items_json TEXT NOT NULL,
                admin_notes TEXT DEFAULT ''
            )
        ''')

        
        # 6. Purchase Orders
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS purchase_orders (
                id {pk_type},
                pr_id INTEGER,
                customer_email TEXT NOT NULL,
                created_at TEXT NOT NULL,
                total_amount REAL NOT NULL,
                items_json TEXT NOT NULL,
                amount_received REAL DEFAULT 0,
                invoice_source TEXT DEFAULT 'PR',
                revision_of_id INTEGER DEFAULT NULL,
                display_id TEXT DEFAULT NULL,
                is_active INTEGER DEFAULT 1,
                revision_reason TEXT DEFAULT NULL,
                status TEXT DEFAULT 'Accepted',
                customer_name_snapshot TEXT,
                business_name_snapshot TEXT,
                address_snapshot TEXT,
                customer_category_snapshot TEXT,
                customer_email_snapshot TEXT,
                customer_mobile_snapshot TEXT,
                expected_delivery_date DATE,
                delivery_stage VARCHAR(50) DEFAULT 'ORDER_PLACED',
                order_placed_at TIMESTAMP,
                packaged_at TIMESTAMP,
                shipped_at TIMESTAMP,
                out_for_delivery_at TIMESTAMP,
                delivered_at TIMESTAMP
            )
        ''')
            
        # 7. Settings
        c.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        
        # 8. Invoices
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS invoices (
                id {pk_type},
                po_id INTEGER NOT NULL,
                invoice_no TEXT NOT NULL,
                created_at TEXT NOT NULL,
                payment_mode TEXT NOT NULL,
                delivery_remarks TEXT,
                items_json TEXT NOT NULL,
                status TEXT DEFAULT 'PENDING',
                FOREIGN KEY (po_id) REFERENCES purchase_orders (id)
            )
        ''')

        # 9. Email Notifications Log
        if is_pg:
            c.execute('''
                CREATE TABLE IF NOT EXISTS email_notifications (
                    id SERIAL PRIMARY KEY,
                    event_type VARCHAR NOT NULL,
                    ref_id INTEGER NOT NULL,
                    recipient_role VARCHAR NOT NULL,
                    recipient_email VARCHAR NOT NULL,
                    status VARCHAR DEFAULT 'SENT',
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    error_message TEXT
                )
            ''')
        else:
            c.execute('''
                CREATE TABLE IF NOT EXISTS email_notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    ref_id INTEGER NOT NULL,
                    recipient_role TEXT NOT NULL,
                    recipient_email TEXT NOT NULL,
                    status TEXT DEFAULT 'SENT',
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    error_message TEXT
                )
            ''')
            
        # 8. OTP Codes - REMOVED: Using Supabase native email verification
        
        # Initial Seeds
        if is_pg:
            c.execute("INSERT INTO settings (key, value) VALUES ('supplier_name', 'Hotel Supplier Inc.') ON CONFLICT (key) DO NOTHING")
        else:
            c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('supplier_name', 'Hotel Supplier Inc.')")
        conn.commit()
        print("[OK] Database initialization complete.")
    except Exception as e:
        print(f"[ERROR] Database initialization FAILED: {e}")
        try:
            conn.rollback()
        except:
            pass
    finally:
        conn.close()

def calculate_next_delivery_date(po_created_at: datetime, delivery_days: list) -> str:
    """
    Calculate next delivery date based on route delivery days.
    
    Args:
        po_created_at: PO creation timestamp
        delivery_days: List of weekday names (e.g., ["MONDAY", "FRIDAY"])
    
    Returns:
        Next delivery date as string (YYYY-MM-DD)
    
    Logic:
        - Convert delivery_days to weekday numbers (0=Monday, 6=Sunday)
        - Find next occurrence of any delivery day
        - If PO created on delivery day, check time:
          - Before cutoff (e.g., 10 AM) → same day
          - After cutoff → next delivery day
    """
    from datetime import datetime, timedelta
    import calendar
    
    # Convert delivery days to weekday numbers
    delivery_weekdays = []
    for day in delivery_days:
        day_map = {
            'MONDAY': 0, 'TUESDAY': 1, 'WEDNESDAY': 2, 'THURSDAY': 3,
            'FRIDAY': 4, 'SATURDAY': 5, 'SUNDAY': 6
        }
        if day in day_map:
            delivery_weekdays.append(day_map[day])
    
    if not delivery_weekdays:
        # Default to next day if no delivery days configured
        return (po_created_at + timedelta(days=1)).strftime('%Y-%m-%d')
    
    po_date = po_created_at.date()
    current_time = po_created_at.time()
    cutoff_time = datetime.strptime('10:00', '%H:%M').time()
    
    # Check each day from tomorrow onwards
    for i in range(1, 8):  # Check up to 7 days ahead
        check_date = po_date + timedelta(days=i)
        check_weekday = check_date.weekday()
        
        if check_weekday in delivery_weekdays:
            if i == 0 and current_time <= cutoff_time:
                # Same day delivery if before cutoff
                return check_date.strftime('%Y-%m-%d')
            else:
                # Future delivery day
                return check_date.strftime('%Y-%m-%d')
    
    # Fallback to next Monday
    days_until_monday = (7 - po_date.weekday()) % 7 or 7
    return (po_date + timedelta(days=days_until_monday)).strftime('%Y-%m-%d')

def get_current_delivery_stage(po: dict, current_date: date = None) -> tuple:
    """
    Calculate current delivery stage based on dates.
    
    Args:
        po: Purchase order dict with delivery timestamps
        current_date: Today's date (defaults to today)
    
    Returns:
        (stage, timestamp) tuple
    """
    from datetime import datetime, date, timedelta
    
    if current_date is None:
        current_date = date.today()
    
    # Priority 1: Already Delivered
    if 'delivered_at' in po and po['delivered_at']:
        return ('DELIVERED', po['delivered_at'])
    
    # Priority 2: Time-based progression (Future/Estimated stages)
    expected_date = None
    if 'expected_delivery_date' in po and po['expected_delivery_date']:
        try:
             expected_date = datetime.strptime(po['expected_delivery_date'], '%Y-%m-%d').date()
        except:
             pass
    
    if expected_date:
        # Day AFTER delivery -> Delivered (End of delivery day)
        if current_date > expected_date:
            return ('DELIVERED', expected_date.strftime('%Y-%m-%d 23:59'))
            
        # Delivery Day -> Out for Delivery
        if current_date == expected_date:
            return ('OUT_FOR_DELIVERY', expected_date.strftime('%Y-%m-%d 00:00'))
        
        # Day before -> Shipped
        if current_date >= expected_date - timedelta(days=1):
            return ('SHIPPED', (expected_date - timedelta(days=1)).strftime('%Y-%m-%d 00:00'))

    # Priority 3: Event-based (Invoice = Packaged)
    # Check if invoice exists (delivery_status='INVOICED') or packaged_at is set
    if (po.get('delivery_status') == 'INVOICED') or ('packaged_at' in po and po['packaged_at']):
        timestamp = po.get('packaged_at')
        if not timestamp and 'created_at' in po:
             # Fallback to slightly after creation if timestamp missing
             timestamp = po['created_at']
        return ('PACKAGED', timestamp)
    
    # Default to order placed
    return ('ORDER_PLACED', po.get('order_placed_at') or po.get('created_at'))

def update_delivery_timestamps(po_id: int):
    """
    Update all delivery stage timestamps for a PO.
    Called daily via background job or on-demand.
    
    Logic:
        - Get current stage from get_current_delivery_stage()
        - Update corresponding timestamp column
        - Do NOT update if already DELIVERED
    """
    from datetime import datetime, date
    
    conn = get_db_connection()
    try:
        # Get PO details
        po = conn.execute("""
            SELECT expected_delivery_date, delivery_stage, created_at, order_placed_at, packaged_at, shipped_at, out_for_delivery_at, delivered_at, delivery_status
            FROM purchase_orders 
            WHERE id = ?
        """, (po_id,)).fetchone()
        
        if not po:
            return
        
        # Skip if already delivered
        # Skip if already delivered
        if po['delivery_stage'] == 'DELIVERED':
            return
        
        # Get current stage
        current_date = date.today()
        stage, timestamp = get_current_delivery_stage(po, current_date)
        
        # Update the appropriate timestamp
        updates = {}
        if stage == 'ORDER_PLACED' and not po['order_placed_at']:
            updates['order_placed_at'] = timestamp
        elif stage == 'PACKAGED' and not po['packaged_at']:
            updates['packaged_at'] = timestamp
        elif stage == 'SHIPPED' and not po['shipped_at']:
            updates['shipped_at'] = timestamp
        elif stage == 'OUT_FOR_DELIVERY' and not po['out_for_delivery_at']:
            updates['out_for_delivery_at'] = timestamp
        elif stage == 'DELIVERED' and not po['delivered_at']:
            updates['delivered_at'] = timestamp
        
        # Update delivery stage
        updates['delivery_stage'] = stage
        
        if updates:
            set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
            values = list(updates.values()) + [po_id]
            
            conn.execute(f"""
                UPDATE purchase_orders 
                SET {set_clause}
                WHERE id = ?
            """, values)
            conn.commit()
            
    finally:
        conn.close()

def apply_migrations():
    """ Runs incremental schema updates safely. """
    conn = get_db_connection()
    c = conn.cursor()
    
    is_pg = DATABASE_URL is not None
    
    # Track migrations applied to avoid repeated scans
    if get_setting('migrations_applied') == 'v11':
        conn.close()
        return

    cols_to_add = {
        'customers': {
            'full_name': 'TEXT',
            'business_name': 'TEXT',
            'mobile': 'TEXT',
            'address': 'TEXT',
            'category': 'TEXT',
            'status': "TEXT DEFAULT 'Active'",
            'supabase_user_id': 'UUID',
            'created_at': "TEXT"
        },
        'products': {
            'image_path': 'TEXT',
            'category_id': 'INTEGER',
            'name_marathi': 'TEXT', 
            'image_url': 'TEXT',    
            'created_at': 'TEXT'    
        },
        'purchase_orders': {
            'amount_received': 'REAL DEFAULT 0',
            'invoice_source': "TEXT DEFAULT 'PR'",
            'revision_of_id': 'INTEGER DEFAULT NULL',
            'display_id': 'TEXT DEFAULT NULL',
            'is_active': 'INTEGER DEFAULT 1',
            'status': "TEXT DEFAULT 'Accepted'",
            'delivery_status': "VARCHAR DEFAULT 'OPEN'",
            'tracking_status': "VARCHAR DEFAULT 'PO_CREATED'",
            'customer_name_snapshot': 'TEXT',
            'business_name_snapshot': 'TEXT',
            'address_snapshot': 'TEXT',
            'customer_category_snapshot': 'TEXT',
            'customer_email_snapshot': 'TEXT',
            'customer_mobile_snapshot': 'TEXT'
        },
        'invoices': {
            'amount_received': 'REAL DEFAULT 0'
        }
    }

    try:
        for table, cols in cols_to_add.items():
            for col, dtype in cols.items():
                if is_pg:
                    try:
                        c.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {dtype}")
                    except Exception as e:
                        print(f"Warning: Migrating {table}.{col} failed: {e}")
                else:
                    try:
                        c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
                    except Exception:
                        pass 

        # Ensure supabase_user_id exists on PostgreSQL even if older deployments skipped cols_to_add
        if is_pg:
            try:
                c.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS supabase_user_id UUID")
            except Exception as e:
                print(f"Warning: Ensuring customers.supabase_user_id failed: {e}")
        
        # New table: customer_categories
        if is_pg:
            c.execute('''
                CREATE TABLE IF NOT EXISTS customer_categories (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    is_active INTEGER DEFAULT 1
                )
            ''')
        else:
            c.execute('''
                CREATE TABLE IF NOT EXISTS customer_categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    is_active INTEGER DEFAULT 1,
                    delivery_days TEXT,
                    parent_id INTEGER,
                    level INTEGER DEFAULT 1,
                    route_name TEXT
                )
            ''')
            
        # Migration: Ensure customer_categories has new columns (route_name, etc)
        try:
             c.execute("ALTER TABLE customer_categories ADD COLUMN delivery_days TEXT")
        except:
             pass
        try:
             c.execute("ALTER TABLE customer_categories ADD COLUMN route_name TEXT")
        except:
             pass
        try:
             c.execute("ALTER TABLE customer_categories ADD COLUMN parent_id INTEGER")
        except:
             pass
        try:
             c.execute("ALTER TABLE customer_categories ADD COLUMN level INTEGER DEFAULT 1")
        except:
             pass

        # New table: email_notifications
        if is_pg:
            c.execute('''
                CREATE TABLE IF NOT EXISTS email_notifications (
                    id SERIAL PRIMARY KEY,
                    event_type VARCHAR NOT NULL,
                    ref_id INTEGER NOT NULL,
                    recipient_role VARCHAR NOT NULL,
                    recipient_email VARCHAR NOT NULL,
                    status VARCHAR DEFAULT 'SENT',
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    error_message TEXT
                )
            ''')
        else:
            c.execute('''
                CREATE TABLE IF NOT EXISTS email_notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    ref_id INTEGER NOT NULL,
                    recipient_role TEXT NOT NULL,
                    recipient_email TEXT NOT NULL,
                    status TEXT DEFAULT 'SENT',
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    error_message TEXT
                )
            ''')

        # Migration: add email_notifications.error_message (safe, no drop/recreate)
        if is_pg:
            try:
                c.execute("ALTER TABLE email_notifications ADD COLUMN IF NOT EXISTS error_message TEXT")
            except Exception as e:
                print(f"Warning: Migrating email_notifications.error_message failed: {e}")
        else:
            try:
                c.execute("ALTER TABLE email_notifications ADD COLUMN error_message TEXT")
            except Exception:
                pass
        
        # Sync: Set default 'Active' for any customers missing it
        try:
            c.execute("UPDATE customers SET status = 'Active' WHERE status IS NULL")
        except:
            pass

        # Backfill snapshots for existing POs
        try:
            rows = c.execute("""
                SELECT po.id, c.name, c.business_name, c.address, c.category, c.email, c.mobile 
                FROM purchase_orders po
                JOIN customers c ON po.customer_email = c.email
                WHERE po.customer_name_snapshot IS NULL
            """).fetchall()
            for row in rows:
                if is_pg:
                    c.execute("""
                        UPDATE purchase_orders 
                        SET customer_name_snapshot = %s, business_name_snapshot = %s, 
                            address_snapshot = %s, customer_category_snapshot = %s,
                            customer_email_snapshot = %s, customer_mobile_snapshot = %s
                        WHERE id = %s
                    """, (row[1], row[2], row[3], row[4], row[5], row[6], row[0]))
                else:
                    c.execute("""
                        UPDATE purchase_orders 
                        SET customer_name_snapshot = ?, business_name_snapshot = ?, 
                            address_snapshot = ?, customer_category_snapshot = ?,
                            customer_email_snapshot = ?, customer_mobile_snapshot = ?
                        WHERE id = ?
                    """, (row[1], row[2], row[3], row[4], row[5], row[6], row[0]))
        except Exception as e:
            print(f"Warning: Backfilling snapshots failed: {e}")

        # OTP migrations removed - using Supabase native email verification

        # Admin Credentials
        current_email = get_setting('admin_email', ADMIN_EMAIL)
        current_pass = get_setting('admin_password', ADMIN_PASSWORD)
        
        if is_pg:
            c.execute("INSERT INTO settings (key, value) VALUES ('admin_email', %s) ON CONFLICT (key) DO NOTHING", (current_email,))
            c.execute("INSERT INTO settings (key, value) VALUES ('admin_password', %s) ON CONFLICT (key) DO NOTHING", (current_pass,))
        else:
            c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('admin_email', ?)", (current_email,))
            c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('admin_password', ?)", (current_pass,))

        # OTP migrations removed - using Supabase native email verification

        conn.commit()
        # Mark migrations as applied
        set_setting('migrations_applied', 'v11')
    except Exception as e:
        print(f"Error during migrations: {e}")
        try:
            conn.rollback()
        except:
            pass
    finally:
        conn.close()





def get_setting(key: str, default: str = "") -> str:
    try:
        conn = get_db_connection()
        res = conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = res.fetchone()
        conn.close()
        return row['value'] if row else default
    except:
        return default

def set_setting(key: str, value: str):
    try:
        conn = get_db_connection()
        if DATABASE_URL:
            conn.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value", (key, value))
        else:
            conn.execute("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error setting {key}: {e}")

# OTP functionality removed - using Supabase native email verification

# Initialize DB on startup
# ...
init_db()
apply_migrations()


# --- Dependencies & Helpers ---

def get_current_user(request: Request):
    return request.session.get("user")

def require_admin(request: Request):
    user = get_current_user(request)
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=303, detail="Not authorized")
    return user

def require_customer(request: Request):
    user = get_current_user(request)
    if not user or user.get("role") != "customer":
        raise HTTPException(status_code=303, detail="Not authorized")
    return user

# Translation helper
def translate_to_marathi(text: str) -> str:
    try:
        from app.utils import translate_text
        return translate_text(text, target_lang='mr', source_lang='en')
    except Exception as e:
        print(f"Translation helper error: {e}")
        # Simple fallback mapping for common words
        fallback_map = {
            'rice': 'तांद',
            'wheat': 'गहू',
            'sugar': 'साखर',
            'oil': 'तेल',
            'milk': 'दूध',
            'water': 'पाणी',
            'salt': 'मीठ'
        }
        return fallback_map.get(text.lower(), "")

# Basic URL validation
def validate_image_url(url: str) -> bool:
    # Regex for a simple URL validation
    regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # domain...
        r'localhost|' # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, url) is not None

def is_quotation_expired(created_at_str: str) -> bool:
    try:
        created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
        return datetime.now() > (created_at + timedelta(days=3))
    except (ValueError, TypeError):
        return False

# --- API: Duplicate Checks ---

@app.get("/api/check-product-duplicate")
async def check_product_duplicate(name: str, unit: str):
    conn = get_db_connection()
    # Normalize by trimming and lowercase
    res = conn.execute("SELECT id FROM products WHERE LOWER(TRIM(name)) = LOWER(TRIM(?)) AND unit = ?", (name, unit)).fetchone()
    conn.close()
    return {"exists": res is not None}

@app.get("/api/check-category-duplicate")
async def check_category_duplicate(name: str):
    conn = get_db_connection()
    res = conn.execute("SELECT id FROM categories WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))", (name,)).fetchone()
    conn.close()
    return {"exists": res is not None}

@app.get("/api/check-customer-duplicate")
async def check_customer_duplicate(email: str):
    conn = get_db_connection()
    res = conn.execute("SELECT email FROM customers WHERE email = ?", (email.strip(),)).fetchone()
    conn.close()
    return {"exists": res is not None}

@app.post("/api/check-invoice-duplicate")
async def check_invoice_duplicate(request: Request):
    try:
        data = await request.json()
        customer_email = data.get("customer_email")
        items_list = data.get("items") # [{product_id, qty, rate}]
        total_amount = data.get("total_amount")
        
        # Date check (today)
        today = datetime.now().strftime("%Y-%m-%d")
        
        conn = get_db_connection()
        # Find all invoices for this customer today with same total
        orders = conn.execute("""
            SELECT items_json FROM purchase_orders 
            WHERE customer_email = ? 
            AND created_at LIKE ? 
            AND ABS(total_amount - ?) < 0.01
        """, (customer_email, f"{today}%", total_amount)).fetchall()
        
        exists = False
        import json
        for order in orders:
            try:
                order_items = json.loads(order['items_json'])
                # Compare products and quantities
                if len(order_items) != len(items_list):
                    continue
                
                # Sort both to compare easily
                canonical_existing = sorted([(int(itm['product_id']), float(itm['qty'])) for itm in order_items])
                canonical_new = sorted([(int(itm['product_id']), float(itm['qty'])) for itm in items_list])
                
                if canonical_existing == canonical_new:
                    exists = True
                    break
            except:
                continue
        
        conn.close()
        return {"exists": exists}
    except Exception as e:
        print(f"Error checking invoice duplicate: {e}")
        return {"exists": False}

@app.post("/api/translate")
async def api_translate(request: Request):
    """Endpoint for frontend translation requests"""
    try:
        data = await request.json()
        text = data.get("text", "")
        target = data.get("target", "mr")
        source = data.get("source", "en")
        
        if not text:
            return JSONResponse(content={"translatedText": ""})
            
        from app.utils import translate_text
        translated = translate_text(text, target_lang=target, source_lang=source)
        return JSONResponse(content={"translatedText": translated})
    except Exception as e:
        print(f"API Translate Error: {e}")
        return JSONResponse(content={"translatedText": text}) # Return original on error

# ============================================================================
# DELIVERY TRACKING FUNCTIONS
# ============================================================================

def calculate_next_delivery_date(po_created_at, delivery_days):
    """
    Calculate next delivery date based on route delivery days.
    
    Args:
        po_created_at: PO creation datetime
        delivery_days: List of weekday names (e.g., ["MONDAY", "FRIDAY"])
    
    Returns:
        Next delivery date (date object)
    """
    if not delivery_days:
        # Default: 7 days from PO creation
        return (po_created_at + timedelta(days=7)).date()
    
    # Convert weekday names to numbers (0=Monday, 6=Sunday)
    weekday_map = {
        "MONDAY": 0, "TUESDAY": 1, "WEDNESDAY": 2, "THURSDAY": 3,
        "FRIDAY": 4, "SATURDAY": 5, "SUNDAY": 6
    }
    
    delivery_weekdays = []
    for day in delivery_days:
        if day.upper() in weekday_map:
            delivery_weekdays.append(weekday_map[day.upper()])
    
    if not delivery_weekdays:
        return (po_created_at + timedelta(days=7)).date()
    
    # Find next delivery day
    current_date = po_created_at.date()
    current_weekday = po_created_at.weekday()
    
    # Check if today is a delivery day and it's before cutoff time (10 AM)
    if current_weekday in delivery_weekdays and po_created_at.hour < 10:
        return current_date
    
    # Find next delivery day
    for i in range(1, 8):
        next_date = current_date + timedelta(days=i)
        if next_date.weekday() in delivery_weekdays:
            return next_date
    
    # Fallback (should never reach here)
    return current_date + timedelta(days=7)


def get_current_delivery_stage(po, current_datetime=None):
    """
    Calculate current delivery stage based on dates.
    
    Args:
        po: Purchase order dict with expected_delivery_date
        current_datetime: Current datetime (defaults to now)
    
    Returns:
        (stage, timestamp) tuple
    """
    if current_datetime is None:
        current_datetime = datetime.now()
    
    current_date = current_datetime.date()
    
    # If invoice exists (delivered_at is set), always return DELIVERED
    # If invoice exists (delivered_at is set), always return DELIVERED
    if po['delivered_at']:
        return ('DELIVERED', po['delivered_at'])
    
    # Get expected delivery date
    expected_delivery_date = po['expected_delivery_date']
    if not expected_delivery_date:
        return ('ORDER_PLACED', po['order_placed_at'] or po['created_at'])
    
    # Convert to date if string
    if isinstance(expected_delivery_date, str):
        expected_delivery_date = datetime.strptime(expected_delivery_date.split()[0], '%Y-%m-%d').date()
    
    D = expected_delivery_date
    
    # Stage logic based on expected delivery date
    if current_date >= D:
        # On or after delivery day → OUT_FOR_DELIVERY
        timestamp = datetime.combine(D, datetime.min.time())
        return ('OUT_FOR_DELIVERY', timestamp)
    # Day before delivery → SHIPPED
    elif current_date >= D - timedelta(days=1):
        timestamp = datetime.combine(D - timedelta(days=1), datetime.min.time())
        return ('SHIPPED', timestamp)
    
    # Calculate order date safely
    order_dt = po['order_placed_at'] or po['created_at']
    if isinstance(order_dt, str):
        try:
            # Handle various formats including millisecond variants
            fmt = '%Y-%m-%d %H:%M:%S'
            if '.' in order_dt: order_dt = order_dt.split('.')[0]
            if order_dt.count(':') > 2: order_dt = ':'.join(order_dt.split(':')[:3])
            order_dt = datetime.strptime(order_dt, fmt)
        except:
            return ('ORDER_PLACED', order_dt) # Fallback to string if parsing fails
            
    if order_dt and isinstance(order_dt, datetime) and current_date >= order_dt.date() + timedelta(days=1):
        # At least 1 day after order → PACKAGED
        timestamp = order_dt + timedelta(days=1)
        return ('PACKAGED', timestamp)
    else:
        # Just placed → ORDER_PLACED
        return ('ORDER_PLACED', order_dt)


def update_delivery_timestamps(po_id):
    """
    Update delivery stage and timestamps for a PO.
    Called daily or on-demand.
    
    Args:
        po_id: Purchase order ID
    """
    conn = get_db_connection()
    
    try:
        po = conn.execute("""
            SELECT id, expected_delivery_date, delivery_stage, created_at,
                   order_placed_at, packaged_at, shipped_at, 
                   out_for_delivery_at, delivered_at
            FROM purchase_orders
            WHERE id = ?
        """, (po_id,)).fetchone()
        
        if not po:
            return
        
        # Convert to dict for easier access
        po = dict(po)
        
        # Don't update if already delivered
        if po['delivery_stage'] == 'DELIVERED':
            return
        
        # Get current stage
        stage, timestamp = get_current_delivery_stage(po)
        
        # Update database
        update_fields = {
            'delivery_stage': stage
        }
        
        # Set appropriate timestamp
        if stage == 'PACKAGED' and not po['packaged_at']:
            update_fields['packaged_at'] = timestamp
        elif stage == 'SHIPPED' and not po['shipped_at']:
            update_fields['shipped_at'] = timestamp
        elif stage == 'OUT_FOR_DELIVERY' and not po['out_for_delivery_at']:
            update_fields['out_for_delivery_at'] = timestamp
        
        # Build UPDATE query
        set_clause = ', '.join([f"{k} = ?" for k in update_fields.keys()])
        values = list(update_fields.values()) + [po_id]
        
        conn.execute(f"""
            UPDATE purchase_orders
            SET {set_clause}
            WHERE id = ?
        """, values)
        
        conn.commit()
    finally:
        conn.close()


def update_all_delivery_stages():
    """Update delivery stages for all active POs (called by background job)"""
    conn = get_db_connection()
    
    try:
        active_pos = conn.execute("""
            SELECT id
            FROM purchase_orders
            WHERE delivery_stage != 'DELIVERED' AND is_active = 1
        """).fetchall()
        
        for po in active_pos:
            update_delivery_timestamps(po['id'])
    finally:
        conn.close()

# --- Routes: Auth ---
# Note: /login route is below


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = get_current_user(request)
    if user:
        if user["role"] == "admin":
            return RedirectResponse(url="/admin/dashboard", status_code=303)
        return RedirectResponse(url="/customer/dashboard", status_code=303)
    
    supplier_name = get_setting("supplier_name", "Hotel Supplier Inc.")
    return templates.TemplateResponse("landing.html", {"request": request, "supplier_name": supplier_name})

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    user = get_current_user(request)
    if user:
         if user["role"] == "admin":
             return RedirectResponse(url="/admin/dashboard", status_code=303)
         return RedirectResponse(url="/customer/dashboard", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(request: Request, email: str = Form(...), password: str = Form(...)):
    # 1. Admin Check - use settings first, then env
    db_admin_email = get_setting("admin_email", ADMIN_EMAIL)
    db_admin_password = get_setting("admin_password", ADMIN_PASSWORD)
    
    if email == db_admin_email and password == db_admin_password:
        request.session["user"] = {"email": email, "role": "admin", "name": "Administrator"}
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    
    # 2. Customer Check (Supabase Auth)
    if not supabase:
        # Fallback to local database authentication for development
        conn = get_db_connection()
        user = conn.execute("SELECT * FROM customers WHERE email = ?", (email,)).fetchone()
        conn.close()
        
        if user and user["password"] == password:
            request.session["user"] = {"email": user["email"], "role": "customer", "name": user["name"]}
            return RedirectResponse(url="/customer/dashboard", status_code=303)
        else:
            return templates.TemplateResponse("login.html", {
                "request": request, 
                "error": "Invalid email or password"
            })
    
    try:
        res = supabase.auth.sign_in_with_password({"email": email, "password": password})
        
        # Check Email Verification
        if not res.user.email_confirmed_at:
            await supabase.auth.sign_out()
            return templates.TemplateResponse("login.html", {
                "request": request, 
                "error": "Please verify your email address to continue."
            })

        # Login Successful - Fetch Local Profile
        conn = get_db_connection()
        user = conn.execute("SELECT * FROM customers WHERE email = ?", (email,)).fetchone()
        conn.close()
        
        if user:
            request.session["user"] = {"email": user["email"], "role": "customer", "name": user["name"]}
            return RedirectResponse(url="/customer/dashboard", status_code=303)
        else:
            # Should not happen if data is synced, but if so:
            return templates.TemplateResponse("login.html", {"request": request, "error": "Account not found locally."})

    except Exception as e:
        print(f"Login Error: {e}")
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid credentials or login failed."})

@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

@app.get("/forgot-password", response_class=HTMLResponse)
async def forgot_password_page(request: Request):
    return templates.TemplateResponse("forgot_password.html", {"request": request})

@app.post("/forgot-password")
async def forgot_password(request: Request, email: str = Form(...)):
    # 1. Check if Admin
    admin_email = get_setting("admin_email", ADMIN_EMAIL)
    
    if email == admin_email:
        # Admin reset flow still needs consideration if we remove OTP
        # But for now, let's focus on Customer flow as primary request
        # If admin needs reset, they might need manual DB access or use Supabase if we migrate admin too?
        # User said "REMOVE custom OTP".
        # I'll output a message for admin: "Please contact support" or similar if critical.
        # Or frankly, just let Supabase handle it if we migrated admin.
        # But admin is not in Supabase usually.
        # I will leave Admin flow as "Not Supported via Email" for now or just generic error 
        # to focus on Customer Supabase flow. 
        # User said "REMOVE custom OTP". 
        pass

    # 2. Supabase Reset Password (Customer)
    if supabase:
        try:
             # Supabase handles the email sending
             # redirect_to should point to a page that handles the hash fragment
             # or just the login page if it's a "magic link" login.
             supabase.auth.reset_password_email(email)
             
             return templates.TemplateResponse("forgot_password.html", {
                "request": request, 
                "message": "If an account exists, a password reset link has been sent to your email."
            })
        except Exception as e:
            print(f"Supabase Reset Error: {e}")
            # Don't reveal error details
            return templates.TemplateResponse("forgot_password.html", {
                "request": request, 
                "message": "If an account exists, a password reset link has been sent to your email."
            })
            
    return templates.TemplateResponse("forgot_password.html", {
        "request": request, 
        "error": "Password reset service unavailable."
    })

# Password reset handled by Supabase native email flow

@app.post("/register")
async def register(
    request: Request, 
    email: str = Form(...), 
    password: str = Form(...), 
    full_name: str = Form(...),
    business_name: str = Form(...),
    mobile: str = Form(...),
    address: str = Form(...)
):
    # Normalize email
    email = email.strip().lower()
    # Validation
    errors = []
    if len(password) < 8: errors.append("Password < 8 chars.")
    if not re.search(r"[0-9]", password): errors.append("Password must have number.")
    
    if errors:
        return templates.TemplateResponse("register.html", {
            "request": request, "error": " ".join(errors),
            "form": {"email": email, "full_name": full_name, "business_name": business_name, "mobile": mobile, "address": address}
        })

    # 1. Supabase Registration
    if not supabase:
        return templates.TemplateResponse("register.html", {
            "request": request, 
            "error": "Registration service temporarily unavailable. Please try again later.",
            "form": {"email": email, "full_name": full_name, "business_name": business_name, "mobile": mobile, "address": address}
        })
    
    try:
        # DEBUG: Log Supabase signup attempt
        print(f"[DEBUG] Attempting Supabase signup for email: {email}")
        
        # 1. Supabase Auth signup with ONLY email and password
        res = supabase.auth.sign_up({
            "email": email, 
            "password": password
        })
        
        print(f"[DEBUG] Supabase signup response: {res}")
        
        # Treat signup as SUCCESS if Supabase returned a user id (OTP/email confirmation happens later)
        if res.user and getattr(res.user, "id", None):
            print(f"[DEBUG] Supabase signup SUCCESS (user created). user_id={res.user.id}")
            print("[DEBUG] Email/OTP verification pending; confirmed_at may be None and session may be None")
            
            # 2. Create Local Record (Sync) with Supabase user ID
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                conn = get_db_connection()
                print(f"[DEBUG] Inserting customer record with Supabase ID: {res.user.id}")
                
                # Insert with Supabase user ID as reference
                if DATABASE_URL:
                    conn.execute("""
                        INSERT INTO customers (email, password, name, full_name, business_name, mobile, address, supabase_user_id, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (email, 'SUPABASE_AUTH', full_name, full_name, business_name, mobile, address, res.user.id, 'Pending Verification'))
                else:
                    conn.execute("""
                        INSERT INTO customers (email, password, name, full_name, business_name, mobile, address, supabase_user_id, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (email, 'SUPABASE_AUTH', full_name, full_name, business_name, mobile, address, res.user.id, 'Pending Verification'))
                
                conn.commit()
                print(f"[DEBUG] Customer record inserted successfully")
                conn.close()
                
            except Exception as db_error:
                print(f"[ERROR] Local DB Sync Error: {db_error}")
                print("[DEBUG] Not rolling back Supabase user; registration remains successful")

            print("[DEBUG] Returning registration success confirmation to user")
            return templates.TemplateResponse(
                "register_confirmation.html",
                {
                    "request": request,
                    "email": email,
                    "message": "Account created. Please verify OTP sent to your email.",
                },
            )
            
        else:
            # Handle Supabase signup failure
            error_msg = "Registration failed"
            if hasattr(res, 'error') and res.error:
                error_msg = f"Registration failed: {res.error.get('message', 'Unknown error')}"
                print(f"[ERROR] Supabase signup error: {res.error}")
            
            return templates.TemplateResponse("register.html", {
                "request": request, 
                "error": error_msg,
                "form": {"email": email, "full_name": full_name, "business_name": business_name, "mobile": mobile, "address": address}
            })
            
    except Exception as e:
        print(f"[ERROR] Supabase Register Exception: {e}")
        return templates.TemplateResponse("register.html", {
            "request": request, 
            "error": f"Registration failed: {str(e)}",
            "form": {"email": email, "full_name": full_name, "business_name": business_name, "mobile": mobile, "address": address}
        })

# REMOVED: /verify/register, /verify/register/resend (Supabase Native Auth)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)

# --- Routes: Admin ---

@app.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    require_admin(request)
    supplier_name = get_setting("supplier_name", "Hotel Supplier Inc.")
    
    # Analytics
    conn = get_db_connection()
    stats = {
        "products": conn.execute("SELECT COUNT(*) FROM products").fetchone()[0],
        "total_prs": conn.execute("SELECT COUNT(*) FROM purchase_requisitions").fetchone()[0],
        "pending": conn.execute("SELECT COUNT(*) FROM purchase_requisitions WHERE status='PR'").fetchone()[0],
        "quoted": conn.execute("SELECT COUNT(*) FROM purchase_requisitions WHERE status='QT'").fetchone()[0],
        "accepted": conn.execute("SELECT COUNT(*) FROM purchase_requisitions WHERE status='ACCEPTED'").fetchone()[0],
        "rejected": conn.execute("SELECT COUNT(*) FROM purchase_requisitions WHERE status='REJECTED'").fetchone()[0],
        "partially_accepted": conn.execute("SELECT COUNT(*) FROM purchase_requisitions WHERE status='PARTIALLY_ACCEPTED'").fetchone()[0],
    }
    conn.close()
    
    return templates.TemplateResponse("admin_dashboard.html", {
        "request": request, 
        "supplier_name": supplier_name,
        "stats": stats
    })

@app.post("/api/translate")
async def translate_text(request: Request):
    try:
        # Check admin auth manually to return JSON error instead of redirect
        user = get_current_user(request)
        if not user or user.get("role") != "admin":
            return JSONResponse(status_code=401, content={"error": "Not authorized"})
        
        data = await request.json()
        text = data.get('text', '')
        if not text:
            return JSONResponse(status_code=400, content={"error": "No text provided"})
        
        # Use MyMemory API for English to Marathi translation
        import httpx
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"https://api.mymemory.translated.net/get?langpair=en|mr&q={text}",
                    headers={"Content-Type": "application/json"}
                )
                print(f"MyMemory response status: {response.status_code}")
                print(f"MyMemory response body: {response.text}")
                
                if response.status_code == 200:
                    result = response.json()
                    translated_text = result.get("responseData", {}).get("translatedText", "")
                    print(f"Extracted translated_text: '{translated_text}'")
                    
                    if translated_text:
                        return JSONResponse({"marathi": translated_text})
                    else:
                        print("MyMemory returned empty translation")
                        return JSONResponse({"marathi": ""})
                else:
                    print(f"MyMemory API error: {response.status_code} - {response.text}")
                    return JSONResponse({"marathi": ""})
        except Exception as api_error:
            print(f"MyMemory API call failed: {api_error}")
            return JSONResponse({"marathi": ""})
            
    except Exception as e:
        print(f"Translation API error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/admin/profile", response_class=HTMLResponse)
async def admin_profile(request: Request):
    require_admin(request)
    supplier_name = get_setting("supplier_name", "Hotel Supplier Inc.")
    admin_email = get_setting("admin_email", ADMIN_EMAIL)
    
    # Feature 2: Email Config
    otp_sender_email = get_setting("otp_sender_email", "")
    otp_password = get_setting("otp_password", "")
    otp_is_configured = bool(otp_password)
    
    return templates.TemplateResponse("admin_profile.html", {
        "request": request,     
        "supplier_name": supplier_name,
        "admin_email": admin_email,
        "otp_sender_email": otp_sender_email,
        "otp_is_configured": otp_is_configured
    })

@app.post("/admin/profile")
async def update_admin_profile(
    request: Request, 
    supplier_name: str = Form(...),
    admin_email: str = Form(...),
    current_password: str = Form(...),
    new_password: str = Form(None),
    otp_sender_email: str = Form(None),
    otp_password: str = Form(None),
    background_tasks: BackgroundTasks = None # Optional for backward compatibility but effectively required
):
    require_admin(request)
    
    # 1. Verify Current Password
    db_pass = get_setting("admin_password", ADMIN_PASSWORD)
    if current_password != db_pass:
        # Re-fetch settings to render page correctly
        otp_sender_email_db = get_setting("otp_sender_email", "")
        otp_is_configured = bool(get_setting("otp_password", ""))
        
        return templates.TemplateResponse("admin_profile.html", {
            "request": request, 
            "error": "Incorrect current password.",
            "supplier_name": supplier_name,
            "admin_email": admin_email,
            "otp_sender_email": otp_sender_email_db,
            "otp_is_configured": otp_is_configured
        })
        
    # 2. Update Basic Info (Non-Critical)
    set_setting("supplier_name", supplier_name)
    
    # Update OTP settings (Non-Critical, already protected by password check)
    if otp_sender_email:
        set_setting("otp_sender_email", otp_sender_email)
    
    if otp_password and otp_password.strip():
        set_setting("otp_password", otp_password)
        
    # 3. Check for Critical Changes (Email or Password)
    db_admin_email = get_setting("admin_email", ADMIN_EMAIL)
    critical_change = False
    
    if admin_email != db_admin_email:
        critical_change = True
        
    if new_password and new_password.strip():
        # Validate rules immediately
        errors = []
        if len(new_password) < 8:
            errors.append("Password < 8 chars.")
        if not re.search(r"[0-9]", new_password):
            errors.append("No number in password.")
        if not re.search(r"[!@#$%^&*(),.?\":{}|<>]", new_password):
            errors.append("No special char in password.")
            
        if errors:
             # Re-fetch settings
             otp_sender_email_db = get_setting("otp_sender_email", "")
             otp_is_configured = bool(get_setting("otp_password", ""))
             
             return templates.TemplateResponse("admin_profile.html", {
                "request": request, 
                "error": "Invalid New Password: " + " ".join(errors),
                "supplier_name": supplier_name,
                "admin_email": admin_email,
                "otp_sender_email": otp_sender_email_db,
                "otp_is_configured": otp_is_configured
            })
        critical_change = True

    if critical_change:
        # Generate OTP
        otp = utils.generate_otp()
        current_email = db_admin_email # Send to CURRENT email
        # Store OTP
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        expires_at = (datetime.now() + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
        
        # Run cleanup
        cleanup_expired_otps()
        
        conn = get_db_connection()
        pending_data = json.dumps({
            "admin_email": admin_email,
            "new_password": new_password
        })
        
        if DATABASE_URL:
            conn.execute("""
                INSERT INTO otp_codes (email, otp, purpose, data, created_at, expires_at) 
                VALUES (%s, %s, %s, %s, %s, %s) 
                ON CONFLICT(email, purpose) DO UPDATE SET 
                otp=EXCLUDED.otp, data=EXCLUDED.data, created_at=EXCLUDED.created_at, expires_at=EXCLUDED.expires_at
            """, (current_email, otp, 'admin_update', pending_data, created_at, expires_at))
        else:
            conn.execute("""
                INSERT INTO otp_codes (email, otp, purpose, data, created_at, expires_at) 
                VALUES (?, ?, ?, ?, ?, ?) 
                ON CONFLICT(email, purpose) DO UPDATE SET 
                otp=excluded.otp, data=excluded.data, created_at=excluded.created_at, expires_at=excluded.expires_at
            """, (current_email, otp, 'admin_update', pending_data, created_at, expires_at))
        conn.commit()
        conn.close()
        
        # Send Email
        sender = get_setting("otp_sender_email", "")
        sender_pass = get_setting("otp_password", "")
        background_tasks.add_task(utils.send_email_otp, current_email, otp, sender, sender_pass)
            
        return RedirectResponse(url="/verify/admin-update", status_code=303)
        
    # If no critical changes
    return templates.TemplateResponse("admin_profile.html", {
        "request": request, 
        "success": "Profile updated successfully.",
        "supplier_name": supplier_name,
        "admin_email": admin_email,
        "otp_sender_email": get_setting("otp_sender_email", ""),
        "otp_is_configured": bool(get_setting("otp_password", ""))
    })

@app.get("/verify/admin-update", response_class=HTMLResponse)
async def verify_admin_update_page(request: Request):
    require_admin(request)
    # OTP sent to CURRENT email
    current_email = get_setting("admin_email", ADMIN_EMAIL)
    
    conn = get_db_connection()
    record = conn.execute("SELECT * FROM otp_codes WHERE email = ? AND purpose = ?", (current_email, 'admin_update')).fetchone()
    conn.close()
    
    if not record:
         return RedirectResponse(url="/admin/profile", status_code=303)
         
    masked = utils.mask_email(current_email)
    
    return templates.TemplateResponse("auth_verify_otp.html", {
        "request": request, 
        "email": masked,
        "target_url": "/verify/admin-update"
    })

@app.post("/verify/admin-update")
async def verify_admin_update(request: Request, otp: str = Form(...)):
    user = require_admin(request)
    current_email = get_setting("admin_email", ADMIN_EMAIL)
    
    # Verify OTP and get pending data from DB
    conn = get_db_connection()
    record = conn.execute("SELECT * FROM otp_codes WHERE email = ? AND purpose = ?", (current_email, 'admin_update')).fetchone()
    
    valid = False
    if record:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if record["otp"] == otp:
            if now < record["expires_at"]:
                valid = True
                
    if not valid or not record or not record["data"]:
        conn.close()
        return templates.TemplateResponse("auth_verify_otp.html", {
            "request": request, 
            "email": utils.mask_email(current_email),
            "target_url": "/verify/admin-update",
            "error": "Invalid or Expired OTP"
        })
        
    # Apply Critical Changes
    try:
        pending = json.loads(record["data"])
        if pending["admin_email"]:
            set_setting("admin_email", pending["admin_email"])
            user["email"] = pending["admin_email"]
            request.session["user"] = user
            
        if pending["new_password"] and pending["new_password"].strip():
            set_setting("admin_password", pending["new_password"])
        
        # Cleanup
        conn.execute("DELETE FROM otp_codes WHERE email = ? AND purpose = ?", (current_email, 'admin_update'))
        conn.commit()
        
    except Exception as e:
        print(f"Error applying admin update: {e}")
        
    conn.close()
    
    return RedirectResponse(url="/admin/profile?success=Credentials Updated", status_code=303)

# Kept for backward compatibility if any old links use it, but logic moved to Profile
@app.post("/admin/settings")
async def update_settings(request: Request, supplier_name: str = Form(...)):
    require_admin(request)
    set_setting("supplier_name", supplier_name)
    return RedirectResponse(url="/admin/dashboard", status_code=303)

@app.post("/admin/categories/add")
async def add_category(request: Request, category_name: str = Form(...)):
    require_admin(request)
    name = category_name.strip()
    if not name:
        # Just redirect back if empty
        return RedirectResponse(url="/admin/products", status_code=303)
        
    conn = get_db_connection()
    try:
        conn.execute("INSERT INTO categories (name) VALUES (?)", (name,))
        conn.commit()
    except (sqlite3.IntegrityError, psycopg2.Error):
        pass # Already exists
    conn.close()
    return RedirectResponse(url="/admin/products", status_code=303)

@app.get("/admin/products", response_class=HTMLResponse)
async def admin_products(request: Request):
    require_admin(request)
    conn = get_db_connection()
    # Left join categories to get name even if category_id is NULL or not found
    query = '''
        SELECT p.*, c.name as category_name 
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        ORDER BY p.id DESC
    '''
    products = conn.execute(query).fetchall()
    categories = conn.execute("SELECT * FROM categories ORDER BY name").fetchall()
    conn.close()
    return templates.TemplateResponse("admin_products.html", {
        "request": request, 
        "products": products,
        "categories": categories
    })

@app.post("/admin/products/add")
async def add_product(
    request: Request,
    name: str = Form(...),
    name_marathi: str = Form(""),
    unit: str = Form(...),
    rate: float = Form(...),
    category_id: str = Form(None),
    favorite_price: str = Form(None),
    image: UploadFile = File(None),
    image_url: str = Form("")
):
    require_admin(request)
    
    # Handle category_id input (empty string -> None)
    cat_id = None
    if category_id and str(category_id).strip():
        cat_id = int(category_id)
        
    fav_price = None
    if favorite_price and str(favorite_price).strip():
        try:
            fav_price = float(favorite_price)
        except ValueError:
            pass
    
    # Auto-translate if empty
    if not name_marathi and name:
        name_marathi = translate_to_marathi(name)
        
    # Validate image URL
    if image_url and not validate_image_url(image_url):
         pass

    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Handle optional image
    filename = None
    if image and image.filename:
        # Save to uploads
        original_name = image.filename
        safe_name = f"{int(datetime.now().timestamp())}_{original_name}"
        # Ensure upload dir
        import os
        upload_dir = "static/uploads"
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir)
            
        file_location = os.path.join(upload_dir, safe_name)
        
        with open(file_location, "wb+") as file_object:
            shutil.copyfileobj(image.file, file_object)
            
        filename = f"uploads/{safe_name}"
    
    conn = get_db_connection()
    conn.execute("INSERT INTO products (name, name_marathi, unit, rate, favorite_price, category_id, image_path, image_url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                 (name, name_marathi, unit, rate, fav_price, cat_id, filename, image_url, created_at))
    conn.commit()
    conn.close()
    
    return RedirectResponse(url="/admin/products", status_code=303)

@app.post("/admin/products/delete")
async def delete_product(request: Request, product_id: int = Form(...)):
    require_admin(request)
    conn = get_db_connection()
    conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/admin/products", status_code=303)

@app.post("/admin/products/edit/{product_id}")
async def edit_product(request: Request, product_id: int):
    require_admin(request)
    try:
        data = await request.json()
        name = data.get('name')
        name_marathi = data.get('name_marathi', '')
        unit = data.get('unit')
        rate = float(data.get('rate', 0))
        favorite_price = data.get('favorite_price')
        category_id = data.get('category_id')
        image_url = data.get('image_url', '')

        if category_id and str(category_id).strip():
            category_id = int(category_id)
        else:
            category_id = None
        
        if not name or not unit or rate < 0:
            return JSONResponse(status_code=400, content={"error": "Invalid input"})

        if str(favorite_price).strip():
             favorite_price = float(favorite_price)
        else:
             favorite_price = None

        conn = get_db_connection()
        conn.execute("UPDATE products SET name=?, name_marathi=?, unit=?, rate=?, favorite_price=?, category_id=?, image_url=? WHERE id=?", 
                    (name, name_marathi, unit, rate, favorite_price, category_id, image_url, product_id))
        conn.commit()
        conn.close()
        
        return JSONResponse(content={"status": "ok"})
    except Exception as e:
        print(f"Error editing product: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/admin/pr_list", response_class=HTMLResponse)
async def admin_pr_list(
    request: Request,
    search: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    require_admin(request)
    conn = get_db_connection()
    
    # Base query
    query = '''
        SELECT pr.*, c.name as customer_name, po.id as po_id
        FROM purchase_requisitions pr
        JOIN customers c ON pr.customer_email = c.email
        LEFT JOIN purchase_orders po ON pr.id = po.pr_id AND po.is_active = 1
    '''
    
    params = []
    where_clauses = []
    
    # 1. Status Filter
    if status:
        where_clauses.append("pr.status = ?")
        params.append(status)
    
    # 2. Date Filters
    if date_from:
        where_clauses.append("pr.created_at >= ?")
        params.append(f"{date_from} 00:00:00")
    if date_to:
        where_clauses.append("pr.created_at <= ?")
        params.append(f"{date_to} 23:59:59")
    
    # 3. Basic Field Search (ID, Customer Name, Email)
    # Note: Product name search is handled in Python later for robustness across DB types
    if search:
        search_term = f"%{search.lower().strip()}%"
        where_clauses.append("(CAST(pr.id AS TEXT) LIKE ? OR LOWER(c.name) LIKE ? OR LOWER(pr.customer_email) LIKE ?)")
        params.extend([search_term, search_term, search_term])
    
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    query += " ORDER BY pr.created_at DESC"
    
    try:
        rows = conn.execute(query, tuple(params)).fetchall()
    except Exception as e:
        print(f"Error executing PR list query: {e}")
        rows = []
    
    # 4. Product Name Filtering (In Python logic to handle JSON content safely)
    filtered_prs = []
    search_lower = search.lower().strip() if search else ""
    
    for row in rows:
        pr = dict(row)
        pr['is_expired'] = is_quotation_expired(pr['created_at']) if pr['status'] == 'QT' else False
        if search_lower:
            # Check if already matched by ID, name or email
            match_basic = (
                search_lower in str(pr['id']) or 
                search_lower in pr['customer_name'].lower() or 
                search_lower in pr['customer_email'].lower()
            )
            
            if match_basic:
                filtered_prs.append(pr)
                continue
            
            # If not matched basic, check product names inside items_json
            try:
                items = json.loads(pr['items_json'])
                for item in items:
                    name = item.get('name', '').lower()
                    if search_lower in name:
                        filtered_prs.append(pr)
                        break
            except:
                pass
        else:
            filtered_prs.append(pr)
    
    # Categorize PRs for the separate tables
    pending_prs = []
    sent_prs = []
    accepted_prs = []
    rejected_prs = []
    partially_accepted_prs = []
    
    for pr in filtered_prs:
        if pr['status'] == 'PR':
            pending_prs.append(pr)
        elif pr['status'] == 'QT':
            sent_prs.append(pr)
        elif pr['status'] == 'ACCEPTED':
            accepted_prs.append(pr)
        elif pr['status'] == 'REJECTED':
            rejected_prs.append(pr)
        elif pr['status'] == 'PARTIALLY_ACCEPTED':
            partially_accepted_prs.append(pr)
            
    conn.close()
    
    return templates.TemplateResponse("admin_pr_list.html", {
        "request": request, 
        "prs": filtered_prs,
        "pending_prs": pending_prs,
        "sent_prs": sent_prs,
        "accepted_prs": accepted_prs,
        "rejected_prs": rejected_prs,
        "partially_accepted_prs": partially_accepted_prs,
        "search": search or "",
        "status": status or "",
        "date_from": date_from or "",
        "date_to": date_to or "",
        "has_results": len(filtered_prs) > 0
    })

@app.get("/admin/quotation/{pr_id}", response_class=HTMLResponse)
async def admin_quotation(request: Request, pr_id: int):
    require_admin(request)
    conn = get_db_connection()
    
    # Fetch PR with Customer Name
    query = '''
        SELECT pr.*, c.name as customer_name 
        FROM purchase_requisitions pr
        JOIN customers c ON pr.customer_email = c.email
        WHERE pr.id = ?
    '''
    pr = conn.execute(query, (pr_id,)).fetchone()
    
    if not pr:
        conn.close()
        return RedirectResponse(url="/admin/pr_list", status_code=303)
        
    items = json.loads(pr["items_json"])
    
    # Hydrate items with current product names (in case they changed, though ID is stable)
    # Also fetch current base rate from products table to suggest to admin
    hydrated_items = []
    for item in items:
        prod = conn.execute("SELECT * FROM products WHERE id = ?", (item['product_id'],)).fetchone()
        if prod:
            hydrated_items.append({
                "product_id": item['product_id'],
                "name": prod['name'],
                "name_marathi": prod['name_marathi'],
                "qty": item['qty'],
                "unit": prod['unit'],
                "unit": prod['unit'],
                "suggested_rate": prod['rate'] # Default to base rate
            })
            
            # FAVORITE COMPATIBILITY LOGIC
            # If customer is favorite, use favorite_price as suggested_rate if available
            is_fav = conn.execute("SELECT is_favorite FROM customers WHERE email = ?", (pr['customer_email'],)).fetchone()
            if is_fav and is_fav['is_favorite'] == 1:
                # Check if product has favorite price
                if prod['favorite_price'] is not None and prod['favorite_price'] > 0:
                     hydrated_items[-1]['suggested_rate'] = prod['favorite_price']
                     hydrated_items[-1]['is_favorite_price'] = True # Marker for UI if needed
            
    conn.close()
    return templates.TemplateResponse("admin_quotation.html", {"request": request, "pr": pr, "items": hydrated_items})

@app.post("/admin/quotation/{pr_id}/submit")
async def submit_quotation(request: Request, pr_id: int):
    require_admin(request)
    form_data = await request.form()
    
    # Process form data to build items with rates
    # Form keys: rate_{product_id}
    
    conn = get_db_connection()
    pr = conn.execute("SELECT * FROM purchase_requisitions WHERE id = ?", (pr_id,)).fetchone()
    if not pr:
        conn.close()
        return RedirectResponse(url="/admin/pr_list", status_code=303)
        
    old_items = json.loads(pr["items_json"])
    new_items = []
    
    for item in old_items:
        pid = item['product_id']
        rate_key = f"rate_{pid}"
        if rate_key in form_data:
            new_rate = float(form_data[rate_key])
            # Hydrate name/unit for permanent record in PR (so if product deleted, record remains)
            prod = conn.execute("SELECT name, name_marathi, unit FROM products WHERE id = ?", (pid,)).fetchone()
            name = prod['name'] if prod else "Unknown"
            name_marathi = prod['name_marathi'] if prod else ""
            unit = prod['unit'] if prod else "N/A"
            
            new_items.append({
                "product_id": pid,
                "name": name,
                "name_marathi": name_marathi,
                "qty": item['qty'],
                "unit": unit,
                "quoted_rate": new_rate,
                "amount": item['qty'] * new_rate,
                "item_status": "PENDING",  # New field for item-level tracking
                "decision_timestamp": None,
                "is_locked": False
            })
            
    conn.execute("UPDATE purchase_requisitions SET status = 'QT', items_json = ? WHERE id = ?", 
                 (json.dumps(new_items), pr_id))
    conn.commit()

    try:
        from app.notifications import send_email_notification, dispatch_pending_notifications

        # Get supplier name for email
        supplier_name = get_setting("supplier_name", "Hotel Supplier Inc.")
        
        # ENHANCED EMAIL TEMPLATE WITH SUPPLIER INFO, PR NUMBER, AND DIRECT LINK
        subject = f"📄 New Quotation Received for PR #{pr_id}"
        message = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0;">
    <div style="max-width: 600px; margin: 20px auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #ffffff;">
        
        <!-- Header -->
        <div style="text-align: center; margin-bottom: 30px;">
            <h1 style="color: #2c3e50; margin: 0; font-size: 24px;">📄 New Quotation Received</h1>
            <p style="color: #7f8c8d; margin: 5px 0 0 0; font-size: 14px;">PR #{pr_id} - Action Required</p>
        </div>
        
        <!-- Supplier Information -->
        <div style="background-color: #e3f2fd; padding: 20px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #2196f3;">
            <h3 style="color: #1565c0; margin: 0 0 15px 0; font-size: 16px;">🏢 Supplier Details</h3>
            <p style="margin: 8px 0;"><strong>Supplier Name:</strong> {supplier_name}</p>
            <p style="margin: 8px 0;"><strong>PR Number:</strong> #{pr_id}</p>
        </div>
        
        <!-- Action Required -->
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #007bff;">
            <h3 style="color: #2c3e50; margin: 0 0 15px 0; font-size: 16px;">📋 Quotation Received</h3>
            <p style="margin: 0 0 15px 0;">A quotation has been submitted by <strong>{supplier_name}</strong> for your purchase request PR #<strong>{pr_id}</strong>.</p>
            <p style="margin: 0 0 15px 0;">Please review the quotation details at your earliest convenience and decide whether to accept or reject the items.</p>
            
            <ul style="margin: 15px 0; padding-left: 20px;">
                <li style="margin: 8px 0;">Review quotation details and pricing</li>
                <li style="margin: 8px 0;">Accept or reject individual items</li>
                <li style="margin: 8px 0;">Confirm your decision</li>
            </ul>
        </div>
        
        <!-- CTA Button -->
        <div style="text-align: center; margin: 30px 0;">
            <a href="{APP_BASE_URL}/customer/quotation/{pr_id}" style="display: inline-block; background-color: #007bff; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 16px;">
                🚀 View Quotation
            </a>
        </div>
        
        <!-- Footer -->
        <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; text-align: center;">
            <p style="color: #6c757d; margin: 5px 0; font-size: 14px;">Thank you for your business!</p>
            <p style="color: #6c757d; margin: 5px 0; font-size: 12px;">This is an automated message. Please do not reply to this email.</p>
        </div>
        
    </div>
</body>
</html>
""".strip()

        notification_id = send_email_notification(
            event_type="QUOTATION_SENT",
            ref_id=int(pr_id),
            recipient_role="customer",
            recipient_email=pr['customer_email'],
            subject=subject,
            message=message,
            send_email=False,
        )

        if notification_id:
            print(f"[INFO] QUOTATION_SENT notification logged with ID: {notification_id}")
        else:
            print(f"[WARN] Failed to log QUOTATION_SENT notification")

        dispatch_pending_notifications()
    except Exception as e:
        print(f"[WARN] QUOTATION_SENT notification log failed (non-blocking): {e}")

    conn.close()
    
    return RedirectResponse(url="/admin/pr_list", status_code=303)

@app.get("/admin/rejected_items", response_class=HTMLResponse)
async def admin_rejected_items(
    request: Request,
    search: Optional[str] = None,
    customer_email: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    require_admin(request)
    conn = get_db_connection()
    
    # Get all customers for filter
    customers = conn.execute("SELECT DISTINCT email, name FROM customers ORDER BY name").fetchall()
    
    # Get all PRs with potential rejected items
    query = '''
        SELECT pr.*, c.name as customer_name 
        FROM purchase_requisitions pr
        JOIN customers c ON pr.customer_email = c.email
        WHERE pr.status IN ('REJECTED', 'PARTIALLY_ACCEPTED')
    '''
    params = []
    if customer_email:
        query += " AND pr.customer_email = ?"
        params.append(customer_email)
    
    if date_from:
        query += " AND pr.created_at >= ?"
        params.append(f"{date_from} 00:00:00")
        
    if date_to:
        query += " AND pr.created_at <= ?"
        params.append(f"{date_to} 23:59:59")
        
    query += " ORDER BY pr.created_at DESC"
    
    prs = conn.execute(query, tuple(params)).fetchall()
    
    # Extract rejected items from each PR and apply text search
    rejected_data = []
    search_lower = search.lower().strip() if search else None
    
    for pr in prs:
        items = json.loads(pr['items_json'])
        for item in items:
            if item.get('item_status') == 'REJECTED':
                # Apply search filter on PR ID, Item Name, or Customer Name if search provided
                match = True
                if search_lower:
                    match = (
                        search_lower in str(pr['id']) or
                        search_lower in item['name'].lower() or
                        search_lower in pr['customer_name'].lower() or
                        search_lower in pr['customer_email'].lower()
                    )
                
                if match:
                    rejected_data.append({
                        'pr_id': pr['id'],
                        'customer_name': pr['customer_name'],
                        'customer_email': pr['customer_email'],
                        'item_name': item['name'],
                        'qty': item['qty'],
                        'unit': item['unit'],
                        'quoted_rate': item.get('quoted_rate', 0),
                        'amount': item.get('amount', 0),
                        'decision_timestamp': item.get('decision_timestamp', 'N/A'),
                        'pr_created_at': pr['created_at']
                    })
    
    conn.close()
    return templates.TemplateResponse("admin_rejected_items.html", {
        "request": request,
        "rejected_items": rejected_data,
        "customers": customers,
        "search": search or "",
        "selected_customer": customer_email or "",
        "date_from": date_from or "",
        "date_to": date_to or ""
    })




# --- Routes: Draft PRs (New Feature) ---

@app.post("/customer/draft_pr/create")
async def create_draft_pr(request: Request):
    user = require_customer(request)
    form_data = await request.form()
    
    # Extract items
    items = []
    # Similar parsing logic as create_pr
    # Check for direct inputs (qty_{product_id})
    for key, value in form_data.items():
        if key.startswith("qty_") and value and float(value) > 0:
            product_id = int(key.split("_")[1])
            qty = float(value)
            items.append({"product_id": product_id, "qty": qty})
            
    if not items:
        # Redirect back if empty
        return RedirectResponse(
            url="/customer/create_pr?error=No items selected", 
            status_code=303
        )
        
    # Create Draft
    conn = get_db_connection()
    items_json = json.dumps(items)
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    cursor = conn.execute(
        "INSERT INTO draft_prs (customer_email, created_at, items_json) VALUES (?, ?, ?)",
        (user['email'], created_at, items_json)
    )
    draft_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return RedirectResponse(url=f"/customer/draft_pr/{draft_id}", status_code=303)

@app.get("/customer/draft_pr/{draft_id}", response_class=HTMLResponse)
async def view_draft_pr(request: Request, draft_id: int):
    user = require_customer(request)
    conn = get_db_connection()
    
    draft = conn.execute(
        "SELECT * FROM draft_prs WHERE id = ? AND customer_email = ?", 
        (draft_id, user['email'])
    ).fetchone()
    
    if not draft:
        conn.close()
        return RedirectResponse(url="/customer/dashboard?error=Draft not found", status_code=303)
        
    items = json.loads(draft['items_json'])
    
    # Hydrate items
    hydrated_items = []
    total_items = 0
    
    for item in items:
        prod = conn.execute("SELECT * FROM products WHERE id = ?", (item['product_id'],)).fetchone()
        if prod:
            hydrated_items.append({
                "product_id": item['product_id'],
                "name": prod['name'],
                "unit": prod['unit'],
                "qty": item['qty'],
                "image_url": prod['image_url']
            })
            total_items += 1
            
    conn.close()
    
    return templates.TemplateResponse("customer_draft_review.html", {
        "request": request, 
        "draft": draft, 
        "items": hydrated_items,
        "total_items": total_items
    })

@app.post("/customer/draft_pr/{draft_id}/update")
async def update_draft_pr(request: Request, draft_id: int):
    user = require_customer(request)
    form_data = await request.form()
    
    # Action: update or remove
    action = form_data.get('action') 
    product_id = int(form_data.get('product_id'))
    
    conn = get_db_connection()
    draft = conn.execute(
        "SELECT * FROM draft_prs WHERE id = ? AND customer_email = ?", 
        (draft_id, user['email'])
    ).fetchone()
    
    if not draft:
        conn.close()
        return RedirectResponse(url="/customer/dashboard", status_code=303)
        
    items = json.loads(draft['items_json'])
    new_items = []
    
    if action == 'remove':
        new_items = [i for i in items if i['product_id'] != product_id]
    elif action == 'update':
        new_qty = float(form_data.get('qty', 0))
        if new_qty > 0:
            found = False
            for i in items:
                if i['product_id'] == product_id:
                    i['qty'] = new_qty
                    found = True
                new_items.append(i)
            if not found:
                # Add if not found (unexpected but safe)
                new_items.append({"product_id": product_id, "qty": new_qty})
        else:
             # Remove if qty 0
             new_items = [i for i in items if i['product_id'] != product_id]
    
    conn.execute(
        "UPDATE draft_prs SET items_json = ? WHERE id = ?",
        (json.dumps(new_items), draft_id)
    )
    conn.commit()
    conn.close()
    
    return RedirectResponse(url=f"/customer/draft_pr/{draft_id}", status_code=303)

@app.post("/customer/draft_pr/{draft_id}/submit")
async def submit_draft_pr(request: Request, draft_id: int):
    user = require_customer(request)
    conn = get_db_connection()
    
    draft = conn.execute(
        "SELECT * FROM draft_prs WHERE id = ? AND customer_email = ?", 
        (draft_id, user['email'])
    ).fetchone()
    
    if not draft:
        conn.close()
        return RedirectResponse(url="/customer/dashboard", status_code=303)
        
    items_json = draft['items_json']
    items = json.loads(items_json)
    
    if not items:
        conn.close()
        return RedirectResponse(url=f"/customer/draft_pr/{draft_id}?error=Empty Draft", status_code=303)

    # CREATE REAL PR
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor = conn.execute(
        "INSERT INTO purchase_requisitions (customer_email, created_at, items_json) VALUES (?, ?, ?)",
        (user['email'], created_at, items_json)
    )
    pr_id = cursor.lastrowid
    
    # DELETE DRAFT
    conn.execute("DELETE FROM draft_prs WHERE id = ?", (draft_id,))
    
    conn.commit()
    conn.close()
    
    # Log notification (optional, existing logic handles admin view)
    print(f"Draft {draft_id} submitted as PR {pr_id}")
    
    return RedirectResponse(url=f"/customer/dashboard?success=PR Created Successfully", status_code=303)

@app.post("/customer/draft_pr/{draft_id}/delete")
async def delete_draft_pr(request: Request, draft_id: int):
    user = require_customer(request)
    conn = get_db_connection()
    conn.execute("DELETE FROM draft_prs WHERE id = ? AND customer_email = ?", (draft_id, user['email']))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/customer/dashboard", status_code=303)


# --- Routes: Customer ---


@app.get("/customer/dashboard", response_class=HTMLResponse)
async def customer_dashboard(request: Request):
    user = require_customer(request)
    conn = get_db_connection()
    
    stats = {
        "total_prs": conn.execute("SELECT COUNT(*) FROM purchase_requisitions WHERE customer_email=?", (user['email'],)).fetchone()[0],
        "received_qt": conn.execute("SELECT COUNT(*) FROM purchase_requisitions WHERE customer_email=? AND status != 'PR'", (user['email'],)).fetchone()[0],
        "accepted": conn.execute("SELECT COUNT(*) FROM purchase_requisitions WHERE customer_email=? AND status='ACCEPTED'", (user['email'],)).fetchone()[0],
        "direct_orders": conn.execute("SELECT COUNT(*) FROM purchase_orders WHERE customer_email=? AND pr_id IS NULL", (user['email'],)).fetchone()[0],
    }
    
    # Fetch Active Drafts
    drafts = conn.execute("SELECT * FROM draft_prs WHERE customer_email=? ORDER BY created_at DESC", (user['email'],)).fetchall()
    
    conn.close()
    
    # Process drafts to add item count
    processed_drafts = []
    for d in drafts:
        dd = dict(d)
        try:
            items = json.loads(dd['items_json'])
            dd['item_count'] = len(items)
        except:
            dd['item_count'] = 0
        processed_drafts.append(dd)
    
    return templates.TemplateResponse("customer_dashboard.html", {
        "request": request, 
        "stats": stats,
        "drafts": processed_drafts
    })

@app.get("/customer/quotations", response_class=HTMLResponse)
async def customer_quotations(request: Request):
    user = require_customer(request)
    customer_email = user['email']
    conn = get_db_connection()
    
    # Fetch PRs
    prs_rows = conn.execute('''
        SELECT pr.*, 
               po.id as po_id,
               inv.id as invoice_id
        FROM purchase_requisitions pr
        LEFT JOIN purchase_orders po ON pr.id = po.pr_id
        LEFT JOIN invoices inv ON po.id = inv.po_id
        WHERE pr.customer_email = ?
        ORDER BY pr.created_at DESC
    ''', (customer_email,)).fetchall()
    

    # Fetch direct orders
    direct_orders_rows = conn.execute('''
        SELECT 
            NULL as id,
            po.customer_email,
            'ACCEPTED' as status,
            po.created_at,
            po.items_json,
            '' as admin_notes,
            po.id as po_id,
            inv.id as invoice_id
        FROM purchase_orders po
        LEFT JOIN invoices inv ON po.id = inv.po_id
        WHERE po.customer_email = ? AND po.pr_id IS NULL AND po.is_active = 1
        ORDER BY po.created_at DESC
    ''', (customer_email,)).fetchall()
    
    # Hydrate with product names for search (Python-side hydration to avoid GROUP_CONCAT)
    all_products = conn.execute("SELECT id, name, name_marathi FROM products").fetchall()
    prod_map = {p['id']: p for p in all_products}
    
    all_rows = []
    for row in list(prs_rows) + list(direct_orders_rows):
        pr = dict(row)
        try:
            items = json.loads(pr['items_json'])
            names = []
            for item in items:
                prod = prod_map.get(item['product_id'])
                if prod:
                    nm = prod['name']
                    if prod['name_marathi']:
                        nm += f" / {prod['name_marathi']}"
                    names.append(nm)
            pr['product_names'] = ", ".join(names)
            pr['is_expired'] = is_quotation_expired(pr['created_at']) if pr['status'] == 'QT' else False
        except:
            pr['product_names'] = ""
            pr['is_expired'] = False
        all_rows.append(pr)
    
    # Sort by created_at desc
    all_rows.sort(key=lambda x: x['created_at'], reverse=True)
    
    conn.close()
    return templates.TemplateResponse("customer_quotation_list.html", {
        "request": request, 
        "prs": all_rows
    })

@app.get("/customer/change-password", response_class=HTMLResponse)
async def customer_change_password_page(request: Request):
    require_customer(request)
    return templates.TemplateResponse("customer_change_password.html", {"request": request})

@app.post("/customer/change-password")
async def customer_change_password(request: Request, current_password: str = Form(...)):
    user = require_customer(request)
    email = user["email"]
    
    # Verify current password
    conn = get_db_connection()
    db_user = conn.execute("SELECT password FROM customers WHERE email = ?", (email,)).fetchone()
    conn.close()
    
    if not db_user or db_user["password"] != current_password:
        return templates.TemplateResponse("customer_change_password.html", {
            "request": request, "error": "Incorrect current password"
        })
    
    # Generate OTP
    otp = utils.generate_otp()
    
    # Store OTP
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    expires_at = (datetime.now() + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    
    conn = get_db_connection()
    conn.execute("INSERT INTO otp_codes (email, otp, created_at, expires_at) VALUES (?, ?, ?, ?) ON CONFLICT(email) DO UPDATE SET otp=excluded.otp, created_at=excluded.created_at, expires_at=excluded.expires_at", 
                 (email, otp, created_at, expires_at))
    conn.commit()
    conn.close()
    
    # Send Email
    sender = get_setting("otp_sender_email", "")
    sender_pass = get_setting("otp_password", "")
    
    if sender and sender_pass:
        utils.send_email_otp(email, otp, sender, sender_pass)
        
    request.session["pending_cust_pass_change"] = True
    
    return RedirectResponse(url="/verify/customer-password", status_code=303)

@app.get("/verify/customer-password", response_class=HTMLResponse)
async def verify_customer_password_page(request: Request):
    user = require_customer(request)
    if not request.session.get("pending_cust_pass_change"):
        return RedirectResponse(url="/customer/change-password", status_code=303)
        
    masked = utils.mask_email(user["email"])
    return templates.TemplateResponse("auth_verify_otp.html", {
        "request": request, 
        "email": masked,
        "target_url": "/verify/customer-password"
    })

@app.post("/verify/customer-password")
async def verify_customer_password(request: Request, otp: str = Form(...)):
    user = require_customer(request)
    if not request.session.get("pending_cust_pass_change"):
        return RedirectResponse(url="/customer/change-password", status_code=303)
        
    email = user["email"]
    
    # Verify OTP
    conn = get_db_connection()
    record = conn.execute("SELECT * FROM otp_codes WHERE email = ?", (email,)).fetchone()
    conn.close()
    
    valid = False
    if record:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if record["otp"] == otp:
            if now < record["expires_at"]:
                valid = True
                
    if not valid:
        return templates.TemplateResponse("auth_verify_otp.html", {
            "request": request, 
            "email": utils.mask_email(email),
            "target_url": "/verify/customer-password",
            "error": "Invalid or Expired OTP"
        })
        
    # Mark Verified
    request.session["pending_cust_pass_change"] = "VERIFIED"
    
    # Clean OTP (optional here, or clean after next step)
    # Keeping OTP valid for few mins or until used is fine, but best to clean up to prevent reuse if we relied on OTP again.
    # Here we rely on session "VERIFIED" state.
    
    return RedirectResponse(url="/customer/set-new-password", status_code=303)

@app.get("/customer/set-new-password", response_class=HTMLResponse)
async def customer_set_password_page(request: Request):
    require_customer(request)
    if request.session.get("pending_cust_pass_change") != "VERIFIED":
        return RedirectResponse(url="/customer/change-password", status_code=303)
    return templates.TemplateResponse("customer_set_password.html", {"request": request})

@app.post("/customer/set-new-password")
async def customer_set_password(request: Request, new_password: str = Form(...), confirm_password: str = Form(...)):
    user = require_customer(request)
    if request.session.get("pending_cust_pass_change") != "VERIFIED":
        return RedirectResponse(url="/customer/change-password", status_code=303)
        
    if new_password != confirm_password:
        return templates.TemplateResponse("customer_set_password.html", {
            "request": request, "error": "Passwords do not match"
        })
        
    # Validate Rules
    errors = []
    if len(new_password) < 8:
        errors.append("Password < 8 chars.")
    if not re.search(r"[0-9]", new_password):
        errors.append("No number.")
    if not re.search(r"[!@#$%^&*(),.?\":{}|<>]", new_password):
        errors.append("No special char.")
        
    if errors:
         return templates.TemplateResponse("customer_set_password.html", {
            "request": request, 
            "error": " ".join(errors)
        })
        
    conn = get_db_connection()
    conn.execute("UPDATE customers SET password = ? WHERE email = ?", (new_password, user["email"]))
    conn.commit()
    # Cleanup OTP just in case
    conn.execute("DELETE FROM otp_codes WHERE email = ?", (user["email"],))
    conn.commit()
    conn.close()
    
    request.session.pop("pending_cust_pass_change", None)
    
    return RedirectResponse(url="/customer/dashboard?success=Password Changed", status_code=303)

@app.get("/customer/profile", response_class=HTMLResponse)
async def customer_profile(request: Request):
    user = require_customer(request)
    conn = get_db_connection()
    # Fetch fresh user data
    db_user = conn.execute("SELECT * FROM customers WHERE email = ?", (user["email"],)).fetchone()
    conn.close()
    
    return templates.TemplateResponse("customer_profile.html", {"request": request, "user": db_user})

@app.post("/customer/profile")
async def update_customer_profile(
    request: Request,
    name: str = Form(...),
    mobile: Optional[str] = Form(None),
    address: Optional[str] = Form(None)
):
    user = require_customer(request)
    email = user["email"].strip().lower()
    conn = get_db_connection()
    
    try:
        conn.execute(
            "UPDATE customers SET name = ?, mobile = ?, address = ? WHERE email = ?", 
            (name, mobile, address, email)
        )
        conn.commit()
        # Update session name
        request.session["user"]["name"] = name
        success = "Profile updated successfully"
    except Exception as e:
        print(f"Error updating profile: {e}")
        success = None
        
    # Fetch fresh user data for re-render
    db_user = conn.execute("SELECT * FROM customers WHERE email = ?", (email,)).fetchone()
    conn.close()
    
    return templates.TemplateResponse("customer_profile.html", {
        "request": request, 
        "user": db_user, 
        "success": success
    })

@app.get("/customer/invoices", response_class=HTMLResponse)
async def customer_invoices(
    request: Request,
    search: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    user = require_customer(request)
    conn = get_db_connection()
    
    # Corrected Query: Fetch from INVOICES table, joined with PO
    query = """
        SELECT 
            i.*, 
            po.display_id as po_display_id, 
            po.total_amount,
            po.tracking_status,
            po.invoice_source
        FROM invoices i
        JOIN purchase_orders po ON i.po_id = po.id
        WHERE po.customer_email = ? AND po.is_active = 1
    """
    params = [user["email"]]
    
    where_clauses = []
    
    if search:
        s = f"%{search.strip()}%"
        where_clauses.append("(CAST(i.invoice_no AS TEXT) LIKE ? OR CAST(po.display_id AS TEXT) LIKE ?)")
        params.extend([s, s])
        
    if date_from:
        where_clauses.append("i.created_at >= ?")
        params.append(f"{date_from} 00:00:00")
        
    if date_to:
        where_clauses.append("i.created_at <= ?")
        params.append(f"{date_to} 23:59:59")
        
    if where_clauses:
        query += " AND " + " AND ".join(where_clauses)
        
    query += " ORDER BY i.created_at DESC"
    
    invoices = conn.execute(query, tuple(params)).fetchall()
    conn.close()
    
    return templates.TemplateResponse("customer_report_invoices.html", {
        "request": request,
        "invoices": invoices,
        "search": search or "",
        "date_from": date_from or "",
        "date_to": date_to or ""
    })

@app.get("/admin/reports", response_class=HTMLResponse)
async def admin_reports(request: Request):
    require_admin(request)
    return templates.TemplateResponse("admin_reports_dashboard.html", {"request": request})

@app.get("/admin/reports/customers", response_class=HTMLResponse)
async def report_customers(
    request: Request,
    search: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 20
):
    require_admin(request)
    conn = get_db_connection()
    
    query = "SELECT * FROM customers"
    params = []
    where_clauses = []
    
    if search:
        s = f"%{search.lower().strip()}%"
        where_clauses.append("(LOWER(name) LIKE ? OR LOWER(email) LIKE ? OR mobile LIKE ?)")
        params.extend([s, s, s])
    
    if category:
        where_clauses.append("category = ?")
        params.append(category)
        
    if status:
        where_clauses.append("status = ?")
        params.append(status)

    if date_from:
        where_clauses.append("created_at >= ?")
        params.append(f"{date_from} 00:00:00")
        
    if date_to:
        where_clauses.append("created_at <= ?")
        params.append(f"{date_to} 23:59:59")
        
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
        
    # Count for pagination
    count_query = query.replace("SELECT *", "SELECT COUNT(*)")
    total_count = conn.execute(count_query, tuple(params)).fetchone()[0]
    
    # Sort and Paginate
    query += " ORDER BY created_at DESC, name ASC"
    query += f" LIMIT {page_size} OFFSET {(page - 1) * page_size}"
    
    customers = conn.execute(query, tuple(params)).fetchall()
    
    # Get all categories from MASTER table
    category_list = conn.execute("SELECT name FROM customer_categories WHERE is_active = 1 ORDER BY name ASC").fetchall()
    
    conn.close()
    
    total_pages = (total_count + page_size - 1) // page_size
    
    return templates.TemplateResponse("admin_report_customers.html", {
        "request": request,
        "customers": customers,
        "search": search or "",
        "category": category or "",
        "category_list": category_list or [],
        "status": status or "",
        "date_from": date_from or "",
        "date_to": date_to or "",
        "total_pages": total_pages,
        "current_page": page,
        "page_size": page_size
    })

@app.post("/admin/customers/toggle_favorite")
async def toggle_customer_favorite(request: Request):
    require_admin(request)
    try:
        data = await request.json()
        email = data.get('email')
        is_favorite = data.get('is_favorite') # true or false
        
        if not email:
            return JSONResponse(status_code=400, content={"error": "Missing email"})
            
        val = 1 if is_favorite else 0
        
        conn = get_db_connection()
        conn.execute("UPDATE customers SET is_favorite = ? WHERE email = ?", (val, email))
        conn.commit()
        conn.close()
        
        return JSONResponse({"status": "ok"})
    except Exception as e:
        print(f"Error toggling favorite: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/admin/reports/customers/update")
async def update_customer_report(request: Request):
    require_admin(request)
    form = await request.form()
    email = form.get("email")
    new_category = form.get("category")
    new_status = form.get("status")
    
    if email:
        conn = get_db_connection()
        conn.execute("UPDATE customers SET category = ?, status = ? WHERE email = ?", (new_category, new_status, email))
        conn.commit()
        conn.close()
        
    return RedirectResponse(url=request.headers.get("referer", "/admin/reports/customers"), status_code=303)

@app.get("/admin/reports/invoices", response_class=HTMLResponse)
async def report_invoices(
    request: Request,
    search: Optional[str] = None,
    customer: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    """Invoice Report - Shows only invoices"""
    require_admin(request)
    conn = get_db_connection()
    
    # Query: Invoices only (no PO join needed)
    query = '''
        SELECT 
            i.id,
            i.invoice_no,
            i.created_at,
            i.payment_mode,
            i.delivery_remarks,
            i.status,
            i.po_id,
            po.display_id,
            po.total_amount,
            c.name as customer_name,
            c.category as customer_category
        FROM invoices i
        JOIN purchase_orders po ON i.po_id = po.id
        JOIN customers c ON po.customer_email = c.email
        WHERE po.is_active = 1
    '''
    
    params = []
    where_clauses = []
    
    if search:
        s = f"%{search.lower().strip()}%"
        where_clauses.append("(LOWER(i.invoice_no) LIKE ? OR LOWER(po.display_id) LIKE ? OR LOWER(c.name) LIKE ?)")
        params.extend([s, s, s])
        
    if customer:
        where_clauses.append("c.email = ?")
        params.append(customer)
        
    if status:
        where_clauses.append("i.status = ?")
        params.append(status)
        
    if date_from:
        where_clauses.append("i.created_at >= ?")
        params.append(f"{date_from} 00:00:00")
        
    if date_to:
        where_clauses.append("i.created_at <= ?")
        params.append(f"{date_to} 23:59:59")
        
    if where_clauses:
        query += " AND " + " AND ".join(where_clauses)
        
    query += " ORDER BY i.created_at DESC"
    
    invoices = conn.execute(query, tuple(params)).fetchall()

    # Get customer list for filter
    customer_list = conn.execute("SELECT name, email FROM customers ORDER BY name").fetchall()
    
    conn.close()
    
    return templates.TemplateResponse("admin_report_invoices.html", {
        "request": request,
        "invoices": invoices,
        "search": search or "",
        "customer": customer or "",
        "status": status or "",
        "date_from": date_from or "",
        "date_to": date_to or "",
        "customer_list": customer_list
    })

@app.get("/admin/reports/pos", response_class=HTMLResponse)
async def report_pos(
    request: Request,
    search: Optional[str] = None,
    customer: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    """PO Report - Shows only purchase orders"""
    require_admin(request)
    conn = get_db_connection()
    
    # Query: POs only (no invoice join needed)
    query = '''
        SELECT 
            po.id, 
            po.display_id,
            po.created_at,
            po.total_amount,
            po.status as pr_status,
            po.delivery_status,
            c.name as customer_name,
            c.category as customer_category,
            c.email as customer_email
        FROM purchase_orders po
        JOIN customers c ON po.customer_email = c.email
        WHERE po.is_active = 1
    '''
    
    params = []
    where_clauses = []
    
    if search:
        s = f"%{search.lower().strip()}%"
        where_clauses.append("(LOWER(po.display_id) LIKE ? OR LOWER(c.name) LIKE ?)")
        params.extend([s, s])
        
    if customer:
        where_clauses.append("c.email = ?")
        params.append(customer)
        
    if status:
        where_clauses.append("po.status = ?")
        params.append(status)
        
    if date_from:
        where_clauses.append("po.created_at >= ?")
        params.append(f"{date_from} 00:00:00")
        
    if date_to:
        where_clauses.append("po.created_at <= ?")
        params.append(f"{date_to} 23:59:59")
        
    if where_clauses:
        query += " AND " + " AND ".join(where_clauses)
        
    query += " ORDER BY po.created_at DESC"
    
    pos = conn.execute(query, tuple(params)).fetchall()

    # Get customer list for filter
    customer_list = conn.execute("SELECT name, email FROM customers ORDER BY name").fetchall()
    
    conn.close()
    
    return templates.TemplateResponse("admin_report_pos.html", {
        "request": request,
        "pos": pos,
        "search": search or "",
        "customer": customer or "",
        "status": status or "",
        "date_from": date_from or "",
        "date_to": date_to or "",
        "customer_list": customer_list
    })

@app.get("/admin/reports/item_sales", response_class=HTMLResponse)
async def report_item_sales(
    request: Request,
    search: Optional[str] = None,
    item_name: Optional[str] = None,
    customer_name: Optional[str] = None,
    category: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    require_admin(request)
    conn = get_db_connection()
    
    # Base query for POs with customer info
    query = '''
        SELECT po.*, c.name as customer_name, c.category as customer_category
        FROM purchase_orders po
        JOIN customers c ON po.customer_email = c.email
        WHERE po.is_active = 1
    '''
    params = []
    where_clauses = []
    
    if date_from:
        where_clauses.append("po.created_at >= ?")
        params.append(f"{date_from} 00:00:00")
    if date_to:
        where_clauses.append("po.created_at <= ?")
        params.append(f"{date_to} 23:59:59")
        
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    query += " ORDER BY po.created_at DESC"
    
    po_rows = conn.execute(query, tuple(params)).fetchall()
    
    # Flatten items and apply remaining filters
    sales_data = []
    all_item_names = set()
    
    # Pre-fetch all product names for the dropdown
    all_products = conn.execute("SELECT name FROM products ORDER BY name ASC").fetchall()
    item_name_list = [p['name'] for p in all_products]

    search_lower = search.lower().strip() if search else None
    
    for po in po_rows:
        try:
            items = json.loads(po['items_json'])
            for item in items:
                # Add to unique item names for future use if needed, but we use master list
                
                # Apply filters
                match = True
                if item_name and item['name'] != item_name: match = False
                if customer_name and customer_name.lower() not in po['customer_name'].lower(): match = False
                if category and po['customer_category'] != category: match = False
                
                if search_lower:
                    search_match = (
                        search_lower in item['name'].lower() or
                        search_lower in po['customer_name'].lower()
                    )
                    if not search_match: match = False
                
                if match:
                    sales_data.append({
                        "date": po['created_at'].split(' ')[0],
                        "item_name": item['name'],
                        "item_marathi": item.get('name_marathi', ''),
                        "qty": item['qty'],
                        "unit": item.get('unit', ''),
                        "customer_name": po['customer_name'],
                        "customer_category": po['customer_category'],
                        "rate": item.get('quoted_rate', 0),
                        "amount": item.get('amount', 0)
                    })
        except:
            continue
            
    # Summary
    total_qty = sum(s['qty'] for s in sales_data)
    total_amount = sum(s['amount'] for s in sales_data)
    
    # Get categories for filter
    categories_rows = conn.execute("SELECT DISTINCT category FROM customers WHERE category IS NOT NULL AND category != ''").fetchall()
    category_list = [c[0] for c in categories_rows]
    
    conn.close()
    
    return templates.TemplateResponse("admin_report_item_sales.html", {
        "request": request,
        "sales": sales_data,
        "total_qty": total_qty,
        "total_amount": total_amount,
        "search": search or "",
        "item_name": item_name or "",
        "customer_name": customer_name or "",
        "category": category or "",
        "date_from": date_from or "",
        "date_to": date_to or "",
        "item_name_list": item_name_list,
        "category_list": category_list
    })




@app.post("/admin/customer_categories/add")
async def add_customer_category(request: Request, name: str = Form(...)):
    require_admin(request)
    conn = get_db_connection()
    try:
        conn.execute("INSERT INTO customer_categories (name) VALUES (?)", (name.strip(),))
        conn.commit()
    except:
        pass # Handle unique constraint silently or with error message
    conn.close()
    return RedirectResponse(url="/admin/customer_categories", status_code=303)

@app.post("/admin/customer_categories/edit")
async def edit_customer_category(request: Request, id: int = Form(...), name: str = Form(...)):
    require_admin(request)
    conn = get_db_connection()
    conn.execute("UPDATE customer_categories SET name = ? WHERE id = ?", (name.strip(), id))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/admin/customer_categories", status_code=303)

@app.post("/admin/customer_categories/toggle")
async def toggle_customer_category(request: Request, id: int = Form(...)):
    require_admin(request)
    conn = get_db_connection()
    cat = conn.execute("SELECT is_active FROM customer_categories WHERE id = ?", (id,)).fetchone()
    if cat:
        new_status = 0 if cat['is_active'] else 1
        conn.execute("UPDATE customer_categories SET is_active = ? WHERE id = ?", (new_status, id))
        conn.commit()
    conn.close()
    return RedirectResponse(url="/admin/customer_categories", status_code=303)

@app.get("/admin/pr_view/{pr_id}", response_class=HTMLResponse)
async def admin_pr_view(request: Request, pr_id: int):
    require_admin(request)
    conn = get_db_connection()
    
    # Fetch PR with Customer info
    query = '''
        SELECT pr.*, c.name as customer_name 
        FROM purchase_requisitions pr
        JOIN customers c ON pr.customer_email = c.email
        WHERE pr.id = ?
    '''
    pr_row = conn.execute(query, (pr_id,)).fetchone()
    
    if not pr_row:
        conn.close()
        return RedirectResponse(url="/admin/reports/prs", status_code=303)
        
    pr = dict(pr_row)
    items = json.loads(pr["items_json"])
    
    # Status Mapping
    db_status = pr['status']
    if db_status == 'PR': pr['display_status'] = 'Submitted'
    elif db_status == 'QT': pr['display_status'] = 'Quotation Sent'
    elif db_status == 'ACCEPTED': pr['display_status'] = 'Invoiced'
    elif db_status == 'PARTIALLY_ACCEPTED': pr['display_status'] = 'Invoiced (Partial)'
    elif db_status == 'REJECTED': pr['display_status'] = 'Rejected'
    else: pr['display_status'] = db_status
    
    # Hydrate items
    hydrated_items = []
    total = 0.0
    for item in items:
        # Check if item has quoted rate/amount
        quoted_rate = item.get('rate') or item.get('quoted_rate')
        
        # Fallback to product rate if not quoted yet
        prod = conn.execute("SELECT name, name_marathi, unit, rate FROM products WHERE id = ?", (item['product_id'],)).fetchone()
        if prod:
            item_name = prod['name']
            item_marathi = prod['name_marathi']
            item_unit = prod['unit']
            if quoted_rate is None:
                quoted_rate = prod['rate']
        else:
            item_name = f"Unknown Product (ID: {item['product_id']})"
            item_marathi = ""
            item_unit = "-"
            if quoted_rate is None:
                quoted_rate = 0.0
                
        qty = item.get('qty', 0.0)
        # Use existing amount or calculate
        amt = item.get('amount')
        if amt is None:
            amt = qty * quoted_rate
            
        hydrated_items.append({
            "product_id": item['product_id'],
            "name": item_name,
            "name_marathi": item_marathi,
            "unit": item_unit,
            "qty": qty,
            "quoted_rate": quoted_rate,
            "amount": amt,
            "item_status": item.get('item_status', 'PENDING')
        })
        
        if item.get('item_status', 'ACCEPTED') == 'ACCEPTED':
            total += amt
            
    conn.close()
    return templates.TemplateResponse("admin_pr_view.html", {
        "request": request, 
        "pr": pr, 
        "items": hydrated_items,
        "total": total
    })

@app.get("/admin/reports/prs", response_class=HTMLResponse)
async def report_prs(
    request: Request,
    search: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    require_admin(request)
    conn = get_db_connection()
    
    query = '''
        SELECT pr.*, c.name as customer_name, c.category as customer_category, po.id as po_id
        FROM purchase_requisitions pr
        LEFT JOIN customers c ON pr.customer_email = c.email
        LEFT JOIN purchase_orders po ON pr.id = po.pr_id AND po.is_active = 1

    '''
    
    params = []
    where_clauses = []
    
    if search:
        s = f"%{search.lower().strip()}%"
        where_clauses.append("(CAST(pr.id AS TEXT) LIKE ? OR LOWER(c.name) LIKE ?)")
        params.extend([s, s])
        
    if category:
        where_clauses.append("c.category = ?")
        params.append(category)
        
    if status:
        # Map friendly status back to DB status if helpful, or just direct filter if they match
        # Derived status mapping in UI: 
        # PR -> Submitted, QT -> Quotation Sent, ACCEPTED -> Invoiced, 
        # PARTIALLY_ACCEPTED -> Invoiced (Partial), REJECTED -> Rejected
        if status == 'Submitted': where_clauses.append("pr.status = 'PR'")
        elif status == 'Quotation Sent': where_clauses.append("pr.status = 'QT'")
        elif status == 'Invoiced': where_clauses.append("pr.status IN ('ACCEPTED', 'PARTIALLY_ACCEPTED')")
        elif status == 'Rejected': where_clauses.append("pr.status = 'REJECTED'")
        else:
            where_clauses.append("pr.status = ?")
            params.append(status)
            
    if date_from:
        where_clauses.append("pr.created_at >= ?")
        params.append(f"{date_from} 00:00:00")
        
    if date_to:
        where_clauses.append("pr.created_at <= ?")
        params.append(f"{date_to} 23:59:59")
        
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
        
    query += " ORDER BY pr.created_at DESC"
    
    prs_rows = conn.execute(query, tuple(params)).fetchall()
    
    prs = []
    for row in prs_rows:
        pr = dict(row)
        if not pr.get('customer_name'):
            pr['customer_name'] = "Deleted Customer"
        # Calculate Amount

        try:
            items = json.loads(pr['items_json'])
            # If rejected/partially accepted, some items might not have amount yet or were rejected.
            # But the requirement for "PR Amount" usually means the total value of items.
            # I'll sum the 'amount' field if present (quoted), else rate * quantity (requested).
            total = 0
            for item in items:
                # amount is set during quotation
                if 'amount' in item:
                    total += item['amount']
                else:
                    # Fallback to rate from product if available, or 0
                    total += item.get('rate', 0) * item.get('quantity', 0)
            pr['total_amount'] = total
        except:
            pr['total_amount'] = 0
            
        # Map Status
        db_status = pr['status']
        if db_status == 'PR': pr['display_status'] = 'Submitted'
        elif db_status == 'QT': 
            if is_quotation_expired(pr['created_at']):
                pr['display_status'] = 'Expired'
            else:
                pr['display_status'] = 'Quotation Sent'
        elif db_status == 'Accepted': pr['display_status'] = 'Invoiced'
        elif db_status == 'Partially Accepted': pr['display_status'] = 'Invoiced (Partial)'
        elif db_status == 'Rejected': pr['display_status'] = 'Rejected'
        else: pr['display_status'] = db_status
        
        prs.append(pr)
        
    # Get categories for filter
    categories = conn.execute("SELECT DISTINCT category FROM customers WHERE category IS NOT NULL AND category != ''").fetchall()
    category_list = [c[0] for c in categories]
    
    conn.close()
    
    return templates.TemplateResponse("admin_report_prs.html", {
        "request": request,
        "prs": prs,
        "search": search or "",
        "category": category or "",
        "status": status or "",
        "date_from": date_from or "",
        "date_to": date_to or "",
        "category_list": category_list
    })

@app.get("/admin/reports/ledger", response_class=HTMLResponse)
async def report_ledger(
    request: Request,
    customer_email: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    require_admin(request)
    conn = get_db_connection()
    
    # Get all customers for the dropdown
    all_customers = conn.execute("SELECT email, name FROM customers ORDER BY name ASC").fetchall()
    
    ledger_entries = []
    selected_customer = None
    
    if customer_email:
        # Get selected customer info
        selected_customer = conn.execute("SELECT * FROM customers WHERE email = ?", (customer_email,)).fetchone()
        
        # Get all POs for this customer, sorted OLD to NEW for ledger
        query = '''
            SELECT po.*, c.category as customer_category
            FROM purchase_orders po
            JOIN customers c ON po.customer_email = c.email
            WHERE po.customer_email = ? AND po.is_active = 1
        '''
        params = [customer_email]
        
        # Note: We need ALL POs to calculate the opening balance correctly, 
        # even if we filter the display by date later.
        query += " ORDER BY po.created_at ASC"
        
        all_pos = conn.execute(query, tuple(params)).fetchall()
        
        running_balance = 0.0
        for po in all_pos:
            opening_bal = running_balance
            inv_amt = po['total_amount']
            amt_rec = po['amount_received'] or 0.0
            closing_bal = opening_bal + inv_amt - amt_rec
            
            entry = {
                "id": po['id'],
                "display_id": po['display_id'] or f"PO-{po['id']}",
                "pr_id": po['pr_id'],
                "date": po['created_at'].split(' ')[0],
                "category": po['customer_category'],
                "opening_bal": opening_bal,
                "inv_amt": inv_amt,
                "amt_rec": amt_rec,
                "closing_bal": closing_bal,
                "raw_date": po['created_at']
            }
            
            # Apply date filter for DISPLAY only
            show = True
            if date_from and entry['date'] < date_from: show = False
            if date_to and entry['date'] > date_to: show = False
            
            if show:
                ledger_entries.append(entry)
                
            running_balance = closing_bal

    conn.close()
    
    return templates.TemplateResponse("admin_report_ledger.html", {
        "request": request,
        "customers": all_customers,
        "selected_email": customer_email or "",
        "selected_customer": selected_customer,
        "ledger": ledger_entries,
        "date_from": date_from or "",
        "date_to": date_to or ""
    })

@app.post("/admin/reports/ledger/update")
async def update_ledger_payment(request: Request):
    require_admin(request)
    form = await request.form()
    po_id = form.get("po_id")
    try:
        amt_rec = float(form.get("amount_received", 0))
    except:
        amt_rec = 0.0
        
    if po_id:
        conn = get_db_connection()
        # Fetch current invoice total and previous balance to validate
        po = conn.execute("SELECT total_amount, customer_email, created_at, delivery_status, tracking_status FROM purchase_orders WHERE id = ?", (po_id,)).fetchone()
        if po:
            # Re-calculate opening balance to properly validate max amount
            all_pos = conn.execute("SELECT total_amount, amount_received, delivery_status FROM purchase_orders WHERE customer_email = ? AND created_at < ? ORDER BY created_at ASC", 
                                   (po['customer_email'], po['created_at'])).fetchall()
            opening_bal = sum((p['total_amount'] - (p['amount_received'] or 0.0)) for p in all_pos)
            
            # Validation
            if amt_rec < 0:
                amt_rec = 0.0
            if amt_rec > (opening_bal + po['total_amount']):
                amt_rec = opening_bal + po['total_amount']
                
            conn.execute("UPDATE purchase_orders SET amount_received = ? WHERE id = ?", (amt_rec, po_id))
            conn.commit()
        conn.close()
        
    return RedirectResponse(url=request.headers.get("referer", "/admin/reports/ledger"), status_code=303)

# --- Admin: Customer Category Management ---

@app.get("/admin/customer_categories", response_class=HTMLResponse)
async def admin_customer_categories(request: Request):
    require_admin(request)
    conn = get_db_connection()
    
    # Get all categories with hierarchy info
    # Note: SQLite doesn't support recursive CTEs easily in all versions, 
    # so we'll fetch all and build hierarchy in Python or just list them with parent info.
    # We need ID for parent selection.
    categories = conn.execute("""
        SELECT 
            c.id, c.name, c.delivery_days, c.route_name, c.parent_id,
            p.name as parent_name
        FROM customer_categories c
        LEFT JOIN customer_categories p ON c.parent_id = p.id
        ORDER BY c.name
    """).fetchall()
    
    # Get customer counts
    category_data = []
    for cat in categories:
        customer_count = conn.execute(
            "SELECT COUNT(*) FROM customers WHERE category = ?", 
            (cat['name'],)
        ).fetchone()[0]
        
        delivery_days = []
        if cat['delivery_days']:
            try:
                delivery_days = json.loads(cat['delivery_days'])
            except:
                delivery_days = []
        
        category_data.append({
            'id': cat['id'],
            'name': cat['name'],
            'parent_id': cat['parent_id'],
            'parent_name': cat['parent_name'],
            'route_name': cat['route_name'] or '',
            'delivery_days': delivery_days,
            'customer_count': customer_count
        })
    
    conn.close()
    
    return templates.TemplateResponse("admin_customer_categories.html", {
        "request": request,
        "categories": category_data
    })

@app.post("/admin/customer_categories/add")
async def add_customer_category(
    request: Request, 
    name: str = Form(...),
    parent_id: int = Form(None),
    route_name: str = Form(None)
):
    require_admin(request)
    conn = get_db_connection()
    try:
        # Calculate level based on parent
        level = 0
        pid = parent_id if parent_id and parent_id > 0 else None
        
        if pid:
            parent = conn.execute("SELECT level FROM customer_categories WHERE id = ?", (pid,)).fetchone()
            if parent:
                level = parent['level'] + 1
        
        # Handle delivery days from form (checkboxes)
        form = await request.form()
        selected_days = []
        days_map = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']
        for day in days_map:
            if form.get(f"day_{day}"):
                selected_days.append(day)
        
        delivery_days_json = json.dumps(selected_days)
        
        conn.execute("""
            INSERT INTO customer_categories (name, parent_id, level, route_name, delivery_days) 
            VALUES (?, ?, ?, ?, ?)
        """, (name.strip(), pid, level, route_name, delivery_days_json))
        conn.commit()
    except Exception as e:
        print(f"Error adding category: {e}")
        # pass # Handle unique constraint silently or with error message
        
    conn.close()
    return RedirectResponse(url="/admin/customer_categories", status_code=303)

@app.post("/admin/customer_categories/update")
async def update_customer_category(request: Request):
    require_admin(request)
    form = await request.form()
    category_name = form.get("category_name")
    route_name = form.get("route_name", "")
    
    # Get selected delivery days from form (multi-select)
    delivery_days = []
    for day in ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']:
        if form.get(f"day_{day}"):
            delivery_days.append(day)
    
    if category_name:
        conn = get_db_connection()
        conn.execute("""
            UPDATE customer_categories
            SET delivery_days = ?, route_name = ?
            WHERE name = ?
        """, (json.dumps(delivery_days), route_name, category_name))
        conn.commit()
        conn.close()
    
    return RedirectResponse(url="/admin/customer_categories", status_code=303)


@app.post("/admin/customer_categories/delete")
async def delete_customer_category(request: Request):
    require_admin(request)
    form = await request.form()
    category_name = form.get("category_name")
    if category_name:
        conn = get_db_connection()
        conn.execute("DELETE FROM customer_categories WHERE name = ?", (category_name,))
        conn.commit()
        conn.close()
    return RedirectResponse(url="/admin/customer_categories", status_code=303)

@app.get("/customer/create_pr", response_class=HTMLResponse)
async def create_pr(request: Request):
    require_customer(request)
    conn = get_db_connection()
    # Join with categories
    query = '''
        SELECT p.*, c.name as category_name 
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        ORDER BY c.name, p.name
    '''
    products = conn.execute(query).fetchall()
    categories = conn.execute("SELECT * FROM categories ORDER BY name").fetchall()
    conn.close()
    return templates.TemplateResponse("create_pr.html", {
        "request": request, 
        "products": products,
        "categories": categories
    })

@app.post("/customer/create_pr")
async def submit_pr(request: Request):
    user = require_customer(request)
    form_data = await request.form()
    
    # Parse quantities
    # Form keys: qty_{product_id}. If > 0, include in PR.
    items = []
    for key, value in form_data.items():
        if key.startswith("qty_") and value:
            try:
                qty = float(value)
                if qty > 0:
                    product_id = int(key.split("_")[1])
                    items.append({"product_id": product_id, "qty": qty})
            except ValueError:
                continue
                
    if not items:
        # Should probably show error, but keeping it simple
        return RedirectResponse(url="/customer/create_pr", status_code=303)
        
    conn = get_db_connection()
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("INSERT INTO purchase_requisitions (customer_email, status, created_at, items_json) VALUES (?, ?, ?, ?)",
                 (user['email'], 'PR', created_at, json.dumps(items)))
    conn.commit()

    try:
        from app.notifications import send_email_notification, dispatch_pending_notifications

        supplier_email = get_setting("admin_email", ADMIN_EMAIL)
        customer_row = conn.execute("SELECT name, full_name, business_name FROM customers WHERE email = ?", (user['email'],)).fetchone()
        customer_name = None
        if customer_row:
            customer_name = customer_row.get('business_name') or customer_row.get('full_name') or customer_row.get('name')
        if not customer_name:
            customer_name = user.get('name') or user.get('email')

        pr_id_row = None
        if DATABASE_URL:
            pr_id_row = conn.execute(
                "SELECT MAX(id) AS id FROM purchase_requisitions WHERE customer_email = %s AND created_at = %s",
                (user['email'], created_at),
            ).fetchone()
        else:
            pr_id_row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()

        pr_id = pr_id_row['id'] if pr_id_row else None
        if pr_id is not None:
            # ENHANCED EMAIL TEMPLATE WITH CUSTOMER INFO, PR NUMBER, AND DIRECT LINK
            subject = f"🔔 New Purchase Request from {customer_name or 'Customer'}"
            body = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0;">
    <div style="max-width: 600px; margin: 20px auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #ffffff;">
        
        <!-- Header -->
        <div style="text-align: center; margin-bottom: 30px;">
            <h1 style="color: #2c3e50; margin: 0; font-size: 24px;">🛒 New Purchase Request</h1>
            <p style="color: #7f8c8d; margin: 5px 0 0 0; font-size: 14px;">PR #{pr_id} - Immediate Action Required</p>
        </div>
        
        <!-- Customer Information -->
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #3498db;">
            <h3 style="color: #2c3e50; margin: 0 0 15px 0; font-size: 16px;">📋 Customer Details</h3>
            <p style="margin: 8px 0;"><strong>Customer Name:</strong> {customer_name or 'N/A'}</p>
            <p style="margin: 8px 0;"><strong>Customer Email:</strong> {user['email']}</p>
            <p style="margin: 8px 0;"><strong>PR Number:</strong> #{pr_id}</p>
        </div>
        
        <!-- Action Required -->
        <div style="background-color: #fff3cd; padding: 20px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #ffc107;">
            <h3 style="color: #856404; margin: 0 0 15px 0; font-size: 16px;">⚡ Action Required</h3>
            <p style="margin: 0 0 15px 0;">A new purchase request has been created and requires your immediate attention. Please review the details and submit your competitive quotation.</p>
            
            <ul style="margin: 0; padding-left: 20px;">
                <li style="margin: 8px 0;">Review purchase request details</li>
                <li style="margin: 8px 0;">Submit your competitive quotation</li>
            </ul>
        </div>
        
        <!-- CTA Button -->
        <div style="text-align: center; margin: 30px 0;">
            <a href="{APP_BASE_URL}/admin/login" style="display: inline-block; background-color: #007bff; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 16px;">
                🚀 View Purchase Request
            </a>
        </div>
        
        <!-- Footer -->
        <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; text-align: center;">
            <p style="color: #6c757d; margin: 5px 0; font-size: 14px;">Prompt response is appreciated to ensure timely processing.</p>
            <p style="color: #6c757d; margin: 5px 0; font-size: 12px;">This is an automated message. Please do not reply to this email.</p>
        </div>
        
    </div>
</body>
</html>
            """.strip()

            notification_id = send_email_notification(
                event_type="PR_CREATED",
                ref_id=int(pr_id),
                recipient_role="supplier",
                recipient_email=supplier_email,
                subject=subject,
                message=body,
            )

            if notification_id:
                print(f"[INFO] PR_CREATED notification logged with ID: {notification_id}")
            else:
                print(f"[WARN] Failed to log PR_CREATED notification")

            dispatch_pending_notifications()
    except Exception as e:
        print(f"[WARN] PR_CREATED notification failed (non-blocking): {e}")

    conn.close()
    
    return templates.TemplateResponse("pr_submitted.html", {"request": request})

# Removed duplicate customer/quotations route

@app.get("/customer/quotation/{pr_id}", response_class=HTMLResponse)
async def view_quotation(request: Request, pr_id: int):
    user = require_customer(request)
    conn = get_db_connection()
    pr = conn.execute("SELECT * FROM purchase_requisitions WHERE id = ? AND customer_email = ?", 
                      (pr_id, user['email'])).fetchone()
    
    if not pr:
        conn.close()
        return RedirectResponse(url="/customer/quotations", status_code=303)
    
    items = json.loads(pr['items_json'])
    total = sum([item.get('amount', 0) for item in items])
    
    # Check for active PO
    po_check = conn.execute("SELECT id FROM purchase_orders WHERE pr_id = ? AND is_active = 1", (pr_id,)).fetchone()
    if po_check:
        # Using purely derived tracking logic below for real-time customer view.
        pass

    # Fetch active PO with delivery tracking data
    po = conn.execute("""
        SELECT id, delivery_status, tracking_status,
               expected_delivery_date, delivery_stage,
               created_at, order_placed_at, packaged_at, shipped_at, 
               out_for_delivery_at, delivered_at
        FROM purchase_orders 
        WHERE pr_id = ? AND is_active = 1
    """, (pr_id,)).fetchone()
    
    po = dict(po) if po else None
    po_id = po['id'] if po else None
    delivery_data = None
    
    # Prepare delivery tracking data if PO exists and order is accepted
    if po and pr['status'] in ['ACCEPTED', 'PARTIALLY_ACCEPTED']:
        from datetime import datetime, date, timedelta
        
        # --- DERIVED TRACKING LOGIC (STRICT) ---
        
        # 1. Gather Basic Data
        current_time = datetime.now()
        current_date = current_time.date()
        
        po_created_at = None
        if po.get('created_at'):
            try:
                po_created_at = datetime.strptime(po['created_at'], '%Y-%m-%d %H:%M:%S')
            except:
                pass
                
        expected_date = None
        if po.get('expected_delivery_date'):
            try:
                expected_date = datetime.strptime(po['expected_delivery_date'], '%Y-%m-%d').date()
            except:
                pass

        # 2. Determine Completed Stages (BOOLEAN LOGIC)
        
        # ORDER PLACED: Always True if PO exists
        stage_order_placed = True
        ts_order_placed = po.get('order_placed_at') or po.get('created_at')

        # PACKAGED: Invoice Exists OR Time Buffer passed
        # Buffer: Let's say 2 hours after PO creation
        stage_packaged = False
        ts_packaged = po.get('packaged_at') # DB value from Invoice creation
        if po.get('delivery_status') == 'INVOICED':
            stage_packaged = True
        elif po_created_at and current_time > po_created_at + timedelta(hours=2):
            stage_packaged = True
            if not ts_packaged:
                 # Derived timestamp: 2 hours after PO
                 ts_packaged = (po_created_at + timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')

        # SHIPPED: Current Date > PO Date AND Current Date < Expected Date
        stage_shipped = False
        ts_shipped = po.get('shipped_at')
        if po_created_at and expected_date:
            # If current date is strictly after PO creation date
            if current_date > po_created_at.date():
                stage_shipped = True
                if not ts_shipped:
                    # Derived: Day before expected or Day after PO
                    # Logic says: Goods in transit. 
                    # Set timestamp to start of day if missing
                    ts_shipped = current_time.strftime('%Y-%m-%d 09:00:00') 
                    # Or better: Day before expected delivery
                    ship_date = expected_date - timedelta(days=1)
                    if ship_date >= po_created_at.date():
                        ts_shipped = ship_date.strftime('%Y-%m-%d 10:00:00')

        # OUT FOR DELIVERY: Current Date == Expected Date
        stage_out_for_delivery = False
        ts_out_for_delivery = po.get('out_for_delivery_at')
        if expected_date and current_date >= expected_date:
             stage_out_for_delivery = True
             if not ts_out_for_delivery:
                 ts_out_for_delivery = expected_date.strftime('%Y-%m-%d 08:00:00')
        
        # DELIVERED: Current Date > Expected Date
        stage_delivered = False
        ts_delivered = po.get('delivered_at')
        if expected_date and current_date > expected_date:
            stage_delivered = True
            if not ts_delivered:
                 ts_delivered = expected_date.strftime('%Y-%m-%d 20:00:00') # End of delivery day

        # 3. Handle Edge Cases / Overrides
        
        # Same day delivery: If expected == PO date, Skip 'Shipped'
        if po_created_at and expected_date and po_created_at.date() == expected_date:
             stage_shipped = True # Mark as done silently or just let logic handle it?
             # If strictly skipping "Shipped" visual, we might need a flag.
             # but "Linear" usually means we show it checked.
             pass

        # 4. Determine Current Active Stage (Highest True)
        current_stage = 'ORDER_PLACED'
        if stage_packaged: current_stage = 'PACKAGED' 
        if stage_shipped and current_date < expected_date: current_stage = 'SHIPPED'
        if stage_out_for_delivery: current_stage = 'OUT_FOR_DELIVERY'
        if stage_delivered: current_stage = 'DELIVERED'

        # 5. Construct Data
        delivery_data = {
            'expected_delivery_date': po['expected_delivery_date'],
            'current_stage': current_stage,
            'stages': {
                'ORDER_PLACED': {
                    'label': 'Order Placed',
                    'timestamp': ts_order_placed,
                    'completed': True # Always completed if we are here
                },
                'PACKAGED': {
                    'label': 'Packaged',
                    'timestamp': ts_packaged if stage_packaged else None,
                    'completed': stage_packaged or stage_shipped or stage_out_for_delivery or stage_delivered
                },
                'SHIPPED': {
                    'label': 'Shipped',
                    'timestamp': ts_shipped if stage_shipped else None,
                    'completed': stage_shipped or stage_out_for_delivery or stage_delivered
                },
                'OUT_FOR_DELIVERY': {
                    'label': 'Out for Delivery',
                    'timestamp': ts_out_for_delivery if stage_out_for_delivery else None,
                    'completed': stage_out_for_delivery or stage_delivered
                },
                'DELIVERED': {
                    'label': 'Delivered',
                    'timestamp': ts_delivered if stage_delivered else None,
                    'completed': stage_delivered
                }
            }
        }
    
    conn.close()
    is_expired = is_quotation_expired(pr['created_at']) if pr['status'] == 'QT' else False
    return templates.TemplateResponse("customer_quotation_view.html", 
                                      {"request": request, "pr": pr, "items": items, "total": total, 
                                       "po_id": po_id, "is_expired": is_expired, "delivery_data": delivery_data})

@app.post("/customer/quotation/{pr_id}/action")
async def quotation_action(request: Request, pr_id: int):
    try:
        print(f"[DEBUG] Quotation action called for PR ID: {pr_id}")
        user = require_customer(request)
        form_data = await request.form()
        print(f"[DEBUG] Form data received: {dict(form_data)}")
        
        conn = get_db_connection()
        pr = conn.execute("SELECT * FROM purchase_requisitions WHERE id = ? AND customer_email = ?", 
                          (pr_id, user['email'])).fetchone()
        
        if not pr:
            print(f"[DEBUG] Quotation not found or access denied for PR ID: {pr_id}, user: {user['email']}")
            conn.close()
            return RedirectResponse(url="/customer/quotations", status_code=303)
        
        print(f"[DEBUG] Quotation found - ID: {pr['id']}, Status: {pr['status']}")
        
        if pr['status'] != 'QT' or is_quotation_expired(pr['created_at']):
            print(f"[DEBUG] Quotation status check failed - Status: {pr['status']}, Expired: {is_quotation_expired(pr['created_at'])}")
            conn.close()
            return RedirectResponse(url="/customer/quotations", status_code=303)
        
        items = json.loads(pr['items_json'])
        
        # Check if items are already locked
        if items and items[0].get('is_locked', False):
            conn.close()
            return RedirectResponse(url="/customer/quotations", status_code=303)
        
        # Process item-level decisions
        # Form keys: item_{product_id}_action = "accept" or "reject"
        decision_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        accepted_count = 0
        rejected_count = 0
        
        for item in items:
            pid = item['product_id']
            action_key = f"item_{pid}_action"
            
            if action_key in form_data:
                action = form_data[action_key]
                if action == "accept":
                    item['item_status'] = "ACCEPTED"
                    accepted_count += 1
                elif action == "reject":
                    item['item_status'] = "REJECTED"
                    rejected_count += 1
                
                item['decision_timestamp'] = decision_timestamp
                item['is_locked'] = True
        
        # Validate: at least one item must be selected
        if accepted_count == 0 and rejected_count == 0:
            conn.close()
            return RedirectResponse(url=f"/customer/quotation/{pr_id}?error=no_selection", status_code=303)
        
        # Determine overall PR status
        if accepted_count > 0 and rejected_count == 0:
            new_status = 'ACCEPTED'
        elif rejected_count > 0 and accepted_count == 0:
            new_status = 'REJECTED' # Rejection is still rejection, but 'Cancelled' might be for POs.
        else:
            new_status = 'PARTIALLY_ACCEPTED'
        
        # Update PR with new status and item decisions
        conn.execute("UPDATE purchase_requisitions SET status = ?, items_json = ? WHERE id = ?", 
                     (new_status, json.dumps(items), pr_id))
        
        if accepted_count > 0:
            # Fetch customer info for snapshots
            customer_row = conn.execute("SELECT name, business_name, address, category, email, mobile FROM customers WHERE email = ?", (user['email'],)).fetchone()
            
            # Create Purchase Order
            accepted_items = [i for i in items if i.get('item_status') == 'ACCEPTED']
            total_amount = sum(float(i.get('amount', 0)) for i in accepted_items)
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Snapshots
            cust_name = customer_row['name'] if customer_row else user['name']
            cust_biz = customer_row['business_name'] if customer_row else ""
            cust_addr = customer_row['address'] if customer_row else ""
            cust_cat = customer_row['category'] if customer_row else ""
            cust_email = customer_row['email'] if customer_row else user['email']
            cust_mobile = customer_row['mobile'] if customer_row else ""

            # Insert PO
            # Note: We must handle both SQLite and PostgreSQL syntax differences via the wrapper if needed, 
            # but standard placeholders usually work if the wrapper handles it.
            # Our wrapper does ? -> %s conversion.
            
            cursor = conn.execute("""
                INSERT INTO purchase_orders (
                    pr_id, customer_email, created_at, total_amount, items_json, 
                    amount_received, invoice_source, status, delivery_status,
                    customer_name_snapshot, business_name_snapshot, address_snapshot, 
                    customer_category_snapshot, customer_email_snapshot, customer_mobile_snapshot,
                    is_active
                ) VALUES (?, ?, ?, ?, ?, 0, 'PR', ?, 'OPEN', ?, ?, ?, ?, ?, ?, 1)
            """, (
                pr_id, user['email'], current_time, total_amount, json.dumps(accepted_items), new_status,
                cust_name, cust_biz, cust_addr, cust_cat, cust_email, cust_mobile
            ))
            
            # Commit the transaction!
            conn.commit()
            
            # ===== DELIVERY TRACKING INTEGRATION =====
            # Get PO ID from cursor
            po_id = cursor.lastrowid if hasattr(cursor, 'lastrowid') else cursor.fetchone()[0]
            
            # Get customer category for delivery route
            delivery_info = conn.execute("""
                SELECT cc.name as category_name, cc.delivery_days, cc.route_name
                FROM customer_categories cc
                WHERE cc.name = ?
            """, (cust_cat,)).fetchone()
            
            delivery_days = []
            if delivery_info and delivery_info['delivery_days']:
                try:
                    delivery_days = json.loads(delivery_info['delivery_days'])
                except:
                    delivery_days = []
            
            # Calculate expected delivery date
            po_created_datetime = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
            expected_delivery_date = calculate_next_delivery_date(po_created_datetime, delivery_days)
            
            # Update PO with delivery tracking fields
            conn.execute("""
                UPDATE purchase_orders
                SET expected_delivery_date = ?,
                    delivery_stage = 'ORDER_PLACED',
                    order_placed_at = ?,
                    customer_category_snapshot = ?
                WHERE id = ?
            """, (expected_delivery_date, current_time, delivery_info['category_name'] if delivery_info else cust_cat, po_id))
            
            conn.commit()
            # ===== END DELIVERY TRACKING =====
            
            from app.notifications import send_email_notification, dispatch_pending_notifications
            from app.notifications import send_email_notification, dispatch_pending_notifications

            supplier_email = get_setting("admin_email", ADMIN_EMAIL)
            
            # Get customer name for email
            customer_row = conn.execute("SELECT name, full_name, business_name FROM customers WHERE email = ?", (user['email'],)).fetchone()
            customer_name = None
            if customer_row:
                customer_name = customer_row['business_name'] or customer_row['full_name'] or customer_row['name']
            if not customer_name:
                customer_name = user['name'] or user['email']
            
            # ENHANCED EMAIL TEMPLATES WITH CUSTOMER INFO, PR NUMBER, AND DIRECT LINK
            if new_status == 'ACCEPTED':
                subject = f"✅ Quotation Accepted by {customer_name}"
                message = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0;">
    <div style="max-width: 600px; margin: 20px auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #ffffff;">
        
        <!-- Header -->
        <div style="text-align: center; margin-bottom: 30px;">
            <h1 style="color: #27ae60; margin: 0; font-size: 24px;">✅ Quotation Accepted</h1>
            <p style="color: #7f8c8d; margin: 5px 0 0 0; font-size: 14px;">PR #{pr_id} - Congratulations!</p>
        </div>
        
        <!-- Customer Information -->
        <div style="background-color: #d4edda; padding: 20px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #28a745;">
            <h3 style="color: #155724; margin: 0 0 15px 0; font-size: 16px;">📋 Acceptance Details</h3>
            <p style="margin: 8px 0;"><strong>Customer Name:</strong> {customer_name or 'N/A'}</p>
            <p style="margin: 8px 0;"><strong>PR Number:</strong> #{pr_id}</p>
            <p style="margin: 8px 0;"><strong>Status:</strong> <span style="color: #155724; font-weight: bold;">Accepted</span></p>
        </div>
        
        <!-- Action Required -->
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #007bff;">
            <h3 style="color: #2c3e50; margin: 0 0 15px 0; font-size: 16px;">🎉 Congratulations!</h3>
            <p style="margin: 0 0 15px 0;">Customer <strong>{customer_name or 'N/A'}</strong> has <strong>accepted</strong> your quotation for PR #<strong>{pr_id}</strong>.</p>
            <p style="margin: 0 0 15px 0;">Your quotation met the customer's requirements and has been approved. Please proceed with the next steps as per your standard business process.</p>
        </div>
        
        <!-- CTA Button -->
        <div style="text-align: center; margin: 30px 0;">
            <a href="{APP_BASE_URL}/admin/login" style="display: inline-block; background-color: #28a745; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 16px;">
                🚀 View Dashboard
            </a>
        </div>
        
        <!-- Footer -->
        <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; text-align: center;">
            <p style="color: #6c757d; margin: 5px 0; font-size: 14px;">Thank you for your excellent service!</p>
            <p style="color: #6c757d; margin: 5px 0; font-size: 12px;">This is an automated message. Please do not reply to this email.</p>
        </div>
        
    </div>
</body>
</html>
                """.strip()
            elif new_status == 'Rejected':
                subject = f"❌ Quotation Rejected by {customer_name}"
                message = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0;">
    <div style="max-width: 600px; margin: 20px auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #ffffff;">
        
        <!-- Header -->
        <div style="text-align: center; margin-bottom: 30px;">
            <h1 style="color: #dc3545; margin: 0; font-size: 24px;">❌ Quotation Rejected</h1>
            <p style="color: #7f8c8d; margin: 5px 0 0 0; font-size: 14px;">PR #{pr_id} - Review Required</p>
        </div>
        
        <!-- Customer Information -->
        <div style="background-color: #f8d7da; padding: 20px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #dc3545;">
            <h3 style="color: #721c24; margin: 0 0 15px 0; font-size: 16px;">📋 Rejection Details</h3>
            <p style="margin: 8px 0;"><strong>Customer Name:</strong> {customer_name or 'N/A'}</p>
            <p style="margin: 8px 0;"><strong>PR Number:</strong> #{pr_id}</p>
            <p style="margin: 8px 0;"><strong>Status:</strong> <span style="color: #721c24; font-weight: bold;">Rejected</span></p>
        </div>
        
        <!-- Action Required -->
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #007bff;">
            <h3 style="color: #2c3e50; margin: 0 0 15px 0; font-size: 16px;">📝 Status Update</h3>
            <p style="margin: 0 0 15px 0;">Customer <strong>{customer_name or 'N/A'}</strong> has <strong>rejected</strong> your quotation for PR #<strong>{pr_id}</strong>.</p>
            <p style="margin: 0 0 15px 0;">The customer has decided not to proceed with this quotation at this time. We encourage you to continue submitting competitive quotations for future opportunities.</p>
        </div>
        
        <!-- CTA Button -->
        <div style="text-align: center; margin: 30px 0;">
            <a href="{APP_BASE_URL}/admin/login" style="display: inline-block; background-color: #dc3545; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 16px;">
                🚀 View Dashboard
            </a>
        </div>
        
        <!-- Footer -->
        <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; text-align: center;">
            <p style="color: #6c757d; margin: 5px 0; font-size: 14px;">Thank you for your participation.</p>
            <p style="color: #6c757d; margin: 5px 0; font-size: 12px;">This is an automated message. Please do not reply to this email.</p>
        </div>
        
    </div>
</body>
</html>
                """.strip()
            else:  # Partially Accepted
                subject = f"⚠️ Quotation Partially Accepted by {customer_name}"
                message = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0;">
    <div style="max-width: 600px; margin: 20px auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #ffffff;">
        
        <!-- Header -->
        <div style="text-align: center; margin-bottom: 30px;">
            <h1 style="color: #ffc107; margin: 0; font-size: 24px;">⚠️ Quotation Partially Accepted</h1>
            <p style="color: #7f8c8d; margin: 5px 0 0 0; font-size: 14px;">PR #{pr_id} - Action Required</p>
        </div>
        
        <!-- Customer Information -->
        <div style="background-color: #fff3cd; padding: 20px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #ffc107;">
            <h3 style="color: #856404; margin: 0 0 15px 0; font-size: 16px;">📋 Partial Acceptance Details</h3>
            <p style="margin: 8px 0;"><strong>Customer Name:</strong> {customer_name or 'N/A'}</p>
            <p style="margin: 8px 0;"><strong>PR Number:</strong> #{pr_id}</p>
            <p style="margin: 8px 0;"><strong>Status:</strong> <span style="color: #856404; font-weight: bold;">Partially Accepted</span></p>
        </div>
        
        <!-- Action Required -->
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #007bff;">
            <h3 style="color: #2c3e50; margin: 0 0 15px 0; font-size: 16px;">📋 Partial Acceptance Explained</h3>
            <p style="margin: 0 0 15px 0;">Customer <strong>{customer_name or 'N/A'}</strong> has <strong>partially accepted</strong> your quotation for PR #<strong>{pr_id}</strong>.</p>
            <p style="margin: 0 0 15px 0;">This means the customer has accepted some items from your quotation while rejecting others. Please review the detailed breakdown in your dashboard to see which items were accepted and which were rejected.</p>
            <p style="margin: 0 0 15px 0;">Please proceed with the accepted items as per your standard business process.</p>
        </div>
        
        <!-- CTA Button -->
        <div style="text-align: center; margin: 30px 0;">
            <a href="{APP_BASE_URL}/admin/login" style="display: inline-block; background-color: #ffc107; color: #212529; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 16px;">
                🚀 View Dashboard
            </a>
        </div>
        
        <!-- Footer -->
        <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; text-align: center;">
            <p style="color: #6c757d; margin: 5px 0; font-size: 14px;">Thank you for your participation.</p>
            <p style="color: #6c757d; margin: 5px 0; font-size: 12px;">This is an automated message. Please do not reply to this email.</p>
        </div>
        
    </div>
</body>
</html>
                """.strip()
            
            notification_id = send_email_notification(
                event_type="QUOTATION_ACCEPTED" if new_status == 'Accepted' else "QUOTATION_REJECTED" if new_status == 'Rejected' else "QUOTATION_PARTIALLY_ACCEPTED",
                ref_id=int(pr_id),
                recipient_role="supplier",
                recipient_email=supplier_email,
                subject=subject,
                message=message,
                send_email=False,
            )

            if notification_id:
                print(f"[INFO] Quotation status notification logged with ID: {notification_id}")
            else:
                print(f"[WARN] Failed to log quotation status notification")

            dispatch_pending_notifications()
        
        # Ensure transaction is committed
        conn.commit()
        conn.close()
        
        if accepted_count > 0:
            return templates.TemplateResponse("po.html", {"request": request, "message": "Purchase Order Created Successfully!"})
        else:
            return RedirectResponse(url="/customer/quotations", status_code=303)
            
    except Exception as e:
        print(f"[ERROR] Quotation action failed: {e}")
        if 'conn' in locals():
            conn.close()
        return RedirectResponse(url="/customer/quotations", status_code=303)

@app.get("/customer/invoices", response_class=HTMLResponse)
async def customer_invoices(
    request: Request,
    search: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    """Customer Invoice List - Shows only generated invoices (not just POs)"""
    user = require_customer(request)
    conn = get_db_connection()
    
    # Query: Get invoices for customer's POs (only actual invoices, not just POs)
    query = '''
        SELECT 
            po.id,
            po.display_id,
            po.created_at,
            po.total_amount,
            po.invoice_source,
            i.invoice_no
        FROM purchase_orders po
        INNER JOIN invoices i ON i.po_id = po.id
        WHERE po.customer_email = ? AND po.is_active = 1
    '''
    
    params = [user['email']]
    where_clauses = []
    
    if search:
        s = f"%{search.lower().strip()}%"
        where_clauses.append("(LOWER(i.invoice_no) LIKE ? OR LOWER(po.display_id) LIKE ?)")
        params.extend([s, s])
        
    if date_from:
        where_clauses.append("po.created_at >= ?")
        params.append(f"{date_from} 00:00:00")
        
    if date_to:
        where_clauses.append("po.created_at <= ?")
        params.append(f"{date_to} 23:59:59")
        
    if where_clauses:
        query += " AND " + " AND ".join(where_clauses)
        
    query += " ORDER BY po.created_at DESC"
    
    invoices = conn.execute(query, tuple(params)).fetchall()
    conn.close()
    
    return templates.TemplateResponse("customer_report_invoices.html", {
        "request": request,
        "invoices": invoices,
        "search": search or "",
        "date_from": date_from or "",
        "date_to": date_to or ""
    })

@app.get("/admin/direct_sales", response_class=HTMLResponse)
async def admin_direct_sales_form(request: Request):
    require_admin(request)
    conn = get_db_connection()
    customers = conn.execute("SELECT email, name FROM customers WHERE status='Active' ORDER BY name").fetchall()
    products = conn.execute("SELECT id, name, unit, rate FROM products ORDER BY name").fetchall()
    conn.close()
    return templates.TemplateResponse("admin_direct_sales.html", {
        "request": request,
        "customers": customers,
        "products": products
    })

@app.post("/admin/direct_sales")
async def admin_direct_sales_submit(
    request: Request,
    customer_email: str = Form(...),
    product_id: List[int] = Form(...),
    qty: List[float] = Form(...),
    rate: List[float] = Form(...)
):
    require_admin(request)
    conn = get_db_connection()
    
    # Validation
    if not product_id or len(product_id) == 0:
        conn.close()
        return RedirectResponse(url="/admin/direct_sales?error=no_items", status_code=303)
        
    items = []
    total_amount = 0
    
    # Fetch customer info for snapshots
    customer = conn.execute("SELECT name, business_name, address, category, email, mobile FROM customers WHERE email = ?", (customer_email,)).fetchone()
    if not customer:
        conn.close()
        return RedirectResponse(url="/admin/direct_sales?error=customer_not_found", status_code=303)

    # Fetch product info for the items_json
    p_ids = ",".join([str(pid) for pid in product_id])
    products_rows = conn.execute(f"SELECT id, name, name_marathi, unit FROM products WHERE id IN ({p_ids})").fetchall()
    prod_map = {p['id']: p for p in products_rows}
    
    for i in range(len(product_id)):
        pid = product_id[i]
        q = qty[i]
        r = rate[i]
        if q > 0:
            prod = prod_map.get(pid)
            if prod:
                amt = q * r
                items.append({
                    "product_id": pid,
                    "name": prod['name'],
                    "name_marathi": prod['name_marathi'],
                    "unit": prod['unit'],
                    "qty": q,
                    "quoted_rate": r,
                    "amount": amt,
                    "item_status": "Accepted",
                    "decision_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "is_locked": True
                })
                total_amount += amt
                
    if not items:
        conn.close()
        return RedirectResponse(url="/admin/direct_sales?error=invalid_items", status_code=303)
        
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Insert Direct Purchase Order (no pr_id)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO purchase_orders 
        (pr_id, customer_email, created_at, total_amount, items_json, invoice_source, delivery_status, tracking_status, status, 
         customer_name_snapshot, business_name_snapshot, address_snapshot, customer_category_snapshot,
         customer_email_snapshot, customer_mobile_snapshot) 
        VALUES (NULL, ?, ?, ?, ?, 'DIRECT', 'OPEN', 'PO_CREATED', 'Accepted', ?, ?, ?, ?, ?, ?)
    ''', (customer_email, created_at, total_amount, json.dumps(items), 
          customer['name'], customer['business_name'], customer['address'], customer['category'],
          customer['email'], customer['mobile']))
    
    print(f"[DEBUG] Direct PO created with delivery_status=OPEN")
    
    po_id = cursor.lastrowid
    display_id = f"PO-{po_id}"
    cursor.execute("UPDATE purchase_orders SET display_id = ? WHERE id = ?", (display_id, po_id))
    
    conn.commit()
    conn.close()
    
    return RedirectResponse(url=f"/admin/orders/create-invoice/{po_id}?success=direct_sales", status_code=303)

@app.get("/admin/orders/revise/{po_id}", response_class=HTMLResponse)
async def revise_invoice_form(request: Request, po_id: int):
    require_admin(request)
    conn = get_db_connection()
    po = conn.execute("SELECT *, delivery_status, tracking_status FROM purchase_orders WHERE id = ?", (po_id,)).fetchone()
    if not po:
        conn.close()
        return Response("PO not found", status_code=404)
        
    products = conn.execute("SELECT id, name, unit, rate FROM products ORDER BY name").fetchall()
    conn.close()
    
    items = json.loads(po['items_json'])
    
    return templates.TemplateResponse("admin_revise_invoice.html", {
        "request": request,
        "po": po,
        "items": items,
        "products": products
    })

@app.post("/admin/orders/revise/{po_id}")
async def revise_invoice_submit(
    request: Request, 
    po_id: int,
    product_id: List[int] = Form(...),
    qty: List[float] = Form(...),
    rate: List[float] = Form(...),
    revision_reason: Optional[str] = Form(None)
):
    require_admin(request)
    conn = get_db_connection()
    
    # 1. Fetch Original PO info
    original_po = conn.execute("SELECT *, delivery_status, tracking_status FROM purchase_orders WHERE id = ?", (po_id,)).fetchone()
    if not original_po:
        conn.close()
        return Response("PO not found", status_code=404)

    # 2. Build New Items List
    p_ids = ",".join([str(pid) for pid in product_id])
    products_rows = conn.execute(f"SELECT id, name, name_marathi, unit FROM products WHERE id IN ({p_ids})").fetchall()
    prod_map = {p['id']: dict(p) for p in products_rows}
    
    # Fetch customer info for snapshots (Re-snapshot during revision to reflect current customer details for the NEW RPO)
    customer = conn.execute("SELECT name, business_name, address, category, email, mobile FROM customers WHERE email = ?", (original_po['customer_email'],)).fetchone()

    items = []
    total_amount = 0
    for i in range(len(product_id)):
        pid = product_id[i]
        q = qty[i]
        r = rate[i]
        if q > 0:
            prod = prod_map.get(pid)
            if prod:
                amt = q * r
                items.append({
                    "product_id": pid,
                    "name": prod['name'],
                    "name_marathi": prod.get('name_marathi', ''),
                    "unit": prod['unit'],
                    "qty": q,
                    "quoted_rate": r,
                    "amount": amt,
                    "item_status": "Accepted", # Revisions are implicitly accepted
                    "is_locked": True
                })
                total_amount += amt

    # 3. Supersede Old PO
    conn.execute("UPDATE purchase_orders SET is_active = 0 WHERE id = ?", (po_id,))
    
    # 4. Determine Root Revision ID
    # If the PO being revised is itself a revision, it has a revision_of_id. Use that.
    # If it's a root PO, its revision_of_id is None, so use its ID.
    root_id = original_po['revision_of_id'] if original_po['revision_of_id'] else po_id
    
    # 5. Generate RPO Number
    # Count how many RPOs exist globally to maintain series RPO-1, RPO-2...
    rpo_count = conn.execute("SELECT count(*) FROM purchase_orders WHERE revision_of_id IS NOT NULL").fetchone()[0]
    next_rpo_num = rpo_count + 1
    display_id = f"RPO-{next_rpo_num}"
    
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 6. Insert Revised PO
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO purchase_orders 
        (pr_id, customer_email, created_at, total_amount, items_json, invoice_source, revision_of_id, display_id, is_active, revision_reason, amount_received, delivery_status, tracking_status, status,
         customer_name_snapshot, business_name_snapshot, address_snapshot, customer_category_snapshot,
         customer_email_snapshot, customer_mobile_snapshot) 
        VALUES (?, ?, ?, ?, ?, 'Revised', ?, ?, 1, ?, ?, 'OPEN', 'PO_CREATED', 'Accepted', ?, ?, ?, ?, ?, ?)
    ''', (original_po['pr_id'], original_po['customer_email'], created_at, total_amount, json.dumps(items), root_id, display_id, revision_reason, original_po['amount_received'],
          customer['name'], customer['business_name'], customer['address'], customer['category'],
          customer['email'], customer['mobile']))
    
    print(f"[DEBUG] Revised PO created with delivery_status=OPEN for PO #{root_id}")
    
    new_po_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return RedirectResponse(url=f"/invoice/po/{new_po_id}", status_code=303)


@app.get("/invoice/po/{po_id}", response_class=HTMLResponse)
async def view_invoice_by_po(request: Request, po_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/", status_code=303)
        
    conn = get_db_connection()
    po_row = conn.execute("SELECT *, delivery_status, tracking_status FROM purchase_orders WHERE id = ?", (po_id,)).fetchone()
    
    if not po_row:
        conn.close()
        return Response("Invoice not found", status_code=404)
        
    po = dict(po_row)
    # Requirement: Invoice must exist in 'invoices' table to be visible
    inv = conn.execute("SELECT * FROM invoices WHERE po_id = ?", (po_id,)).fetchone()
    if not inv:
        if user and user.get('role') == 'admin':
            conn.close()
            return RedirectResponse(url=f"/admin/orders/create-invoice/{po_id}")
        conn.close()
        return Response("Invoice not yet generated by supplier.", status_code=404)
        
    po['invoice_no'] = inv['invoice_no']
    po['payment_mode'] = inv['payment_mode']
    po['delivery_remarks'] = inv['delivery_remarks']
    # If the invoice record has its own items_json (to support revisions/snapshots), use it
    if inv['items_json']:
        po['items_json'] = inv['items_json']

    if user['role'] == 'customer':
        if po['customer_email'] != user['email']:
            conn.close()
            return Response("Unauthorized", status_code=403)
        # Customer should NOT see inactive invoices if they are superseded?
        # Requirement: "Customer can view: ONLY the latest revised invoice (never the original once revised)"
        if po['is_active'] == 0:
             # Find the active one?
             # But if they click a link to an old one, maybe show a warning or redirect?
             # For now, let's allow viewing but maybe show "Superseded" banner.
             # Strict reading: "never the original".
             # Let's try to find the latest revision for this root.
             # Find the active revision for this root
             root_id = po['revision_of_id'] if po['revision_of_id'] else po['id']
             active_po = conn.execute('''
                SELECT id, delivery_status, tracking_status FROM purchase_orders 
                WHERE (id = ? OR revision_of_id = ?) AND is_active = 1
                LIMIT 1
             ''', (root_id, root_id)).fetchone()
             
             if active_po:
                 conn.close()
                 return RedirectResponse(url=f"/invoice/po/{active_po['id']}", status_code=303)

    raw_customer = conn.execute("SELECT * FROM customers WHERE email = ?", (po['customer_email'],)).fetchone()
    
    # Snapshot fallback logic
    customer = {
        "name": po['customer_name_snapshot'] if po['customer_name_snapshot'] else (raw_customer['name'] if raw_customer else "Unknown"),
        "business_name": po['business_name_snapshot'] if po['business_name_snapshot'] else (raw_customer['business_name'] if raw_customer else ""),
        "address": po['address_snapshot'] if po['address_snapshot'] else (raw_customer['address'] if raw_customer else ""),
        "category": po['customer_category_snapshot'] if po['customer_category_snapshot'] else (raw_customer['category'] if raw_customer else ""),
        "email": po['customer_email_snapshot'] if po['customer_email_snapshot'] else (raw_customer['email'] if raw_customer else ""),
        "mobile": po['customer_mobile_snapshot'] if po['customer_mobile_snapshot'] else (raw_customer['mobile'] if raw_customer else (raw_customer['phone'] if raw_customer else ""))
    }

    supplier_name = get_setting("supplier_name", "Hotel Supplier Inc.")
    items = json.loads(po['items_json'])
    
    # Calculate Outstanding Balance (Dynamic)
    # "Previous Balance" = Sum of (Total - Received) for all Previous POs
    
    # 1. Fetch all previous purchase orders for this customer
    # Strict inequality to exclude current timestamp (if created sequentially)
    # But usually unique ID is helper if timestamps match. Using created_at first.
    prev_pos = conn.execute('''
        SELECT total_amount, amount_received 
        FROM purchase_orders 
        WHERE customer_email = ? AND created_at < ? AND is_active = 1
    ''', (po['customer_email'], po['created_at'])).fetchall()
    
    previous_balance = 0.0
    for p in prev_pos:
        amt = p['total_amount']
        rec = p['amount_received'] or 0.0
        previous_balance += (amt - rec)
        
    # "Total Outstanding" = Previous Balance + This Invoice Amount
    # (Assuming this invoice has not been paid yet. If amount_received > 0 on this invoice, 
    # the 'Net Payable' might be different, but 'Total Outstanding' implies Gross Debt context usually)
    # The requirement: "Total Outstanding Balance = Previous Balance + Invoice Amount"
    
    total_outstanding = previous_balance + po['total_amount']

    amount_in_words = number_to_words(total_outstanding)
    
    conn.close()
    
    return templates.TemplateResponse("invoice.html", {
        "request": request, 
        "po": po, 
        "customer": customer, 
        "items": items, 
        "supplier_name": supplier_name,
        "total": po['total_amount'],
        "previous_balance": previous_balance,
        "outstanding_balance": total_outstanding,
        "amount_in_words": amount_in_words,
        "is_revision_mode": (po['is_active'] == 0), 
        "is_admin": (user['role'] == 'admin')
    })

# --- Routes: Order Tracking ---

@app.post("/admin/orders/update-tracking/{po_id}")
async def update_order_tracking(request: Request, po_id: int):
    """Supplier-only endpoint to update tracking status"""
    require_admin(request)
    
    form_data = await request.form()
    new_tracking_status = form_data.get("tracking_status")
    
    # Validate tracking status
    valid_statuses = ['PO_CREATED', 'PROCESSING', 'OUT_FOR_DELIVERY', 'DELIVERED']
    if new_tracking_status not in valid_statuses:
        return JSONResponse(
            status_code=400, 
            content={"error": f"Invalid tracking status. Must be one of: {', '.join(valid_statuses)}"}
        )
    
    conn = get_db_connection()
    
    # Check if PO exists and is OPEN
    po = conn.execute("SELECT id, delivery_status, tracking_status FROM purchase_orders WHERE id = ?", (po_id,)).fetchone()
    if not po:
        conn.close()
        return JSONResponse(status_code=404, content={"error": "Purchase Order not found"})
    
    if po['delivery_status'] != 'OPEN':
        conn.close()
        return JSONResponse(status_code=400, content={"error": "Cannot update tracking status. Order is not OPEN."})
    
    # Validate status progression (can only move forward)
    status_order = ['PO_CREATED', 'PROCESSING', 'OUT_FOR_DELIVERY', 'DELIVERED']
    current_index = status_order.index(po['tracking_status'])
    new_index = status_order.index(new_tracking_status)
    
    if new_index <= current_index:
        conn.close()
        return JSONResponse(status_code=400, content={"error": "Can only move tracking status forward."})
    
    # Update tracking status
    try:
        conn.execute("UPDATE purchase_orders SET tracking_status = ? WHERE id = ?", (new_tracking_status, po_id))
        conn.commit()
        
        print(f"[DEBUG] Tracking status updated for PO #{po_id}: {po['tracking_status']} → {new_tracking_status}")
        
        conn.close()
        return JSONResponse(content={
            "success": True,
            "po_id": po_id,
            "old_status": po['tracking_status'],
            "new_status": new_tracking_status
        })
    except Exception as e:
        conn.close()
        print(f"[ERROR] Failed to update tracking status for PO #{po_id}: {e}")
        return JSONResponse(status_code=500, content={"error": "Failed to update tracking status"})

# --- Routes: Invoice Creation at Delivery ---

@app.get("/admin/orders/{po_id}")
async def admin_view_po(request: Request, po_id: int):
    require_admin(request)
    return RedirectResponse(url=f"/invoice/po/{po_id}")

@app.get("/admin/orders/create-invoice/{po_id}", response_class=HTMLResponse)
async def create_invoice_form(request: Request, po_id: int):
    """Show invoice creation form for supplier"""
    user = require_admin(request)
    conn = get_db_connection()
    
    # Fetch PO with validation
    po = conn.execute('''
        SELECT po.*, c.name as customer_name, c.business_name
        FROM purchase_orders po
        LEFT JOIN customers c ON po.customer_email = c.email
        WHERE po.id = ? AND po.is_active = 1 AND po.delivery_status = 'OPEN'
    ''', (po_id,)).fetchone()
    
    if not po:
        conn.close()
        return templates.TemplateResponse("admin_dashboard.html", {
            "request": request,
            "error": "Purchase Order not found or cannot be invoiced"
        })
    
    # Parse items for pre-filling
    try:
        items = json.loads(po['items_json'])
    except:
        items = []
    
    conn.close()
    return templates.TemplateResponse("create_invoice.html", {
        "request": request,
        "po": po,
        "items": items
    })

@app.post("/admin/orders/create-invoice/{po_id}")
async def create_invoice_submit(request: Request, po_id: int):
    """Process invoice creation at delivery"""
    user = require_admin(request)
    form_data = await request.form()
    
    conn = get_db_connection()
    
    try:
        # Validate PO is still open
        po = conn.execute('''
            SELECT delivery_status FROM purchase_orders 
            WHERE id = ? AND is_active = 1
        ''', (po_id,)).fetchone()
        
        if not po or po['delivery_status'] != 'OPEN':
            conn.close()
            return JSONResponse(status_code=400, content={"error": "Purchase Order cannot be invoiced"})
        
        # Get form data
        payment_mode = form_data.get('payment_mode', 'CASH')
        amount_received = float(form_data.get('amount_received', 0) or 0)
        delivery_remarks = form_data.get('delivery_remarks', '')
        
        # Parse and update items
        po_full = conn.execute('''
            SELECT items_json FROM purchase_orders 
            WHERE id = ? AND is_active = 1
        ''', (po_id,)).fetchone()
        
        items = json.loads(po_full['items_json'])
        updated_items = []
        final_invoice_total = 0.0
        
        for i, item in enumerate(items):
            # Update delivered quantity from form
            delivered_qty_key = f"delivered_qty_{i}"
            delivered_qty = float(form_data.get(delivered_qty_key, item.get('qty', item.get('quoted_qty', 0))))
            
            # Recalculate amount based on delivered quantity
            rate = float(item.get('quoted_rate', 0))
            new_amount = delivered_qty * rate
            
            # Update item with delivered quantity and recalculated amount
            updated_item = item.copy()
            updated_item['delivered_qty'] = delivered_qty
            updated_item['amount'] = new_amount  # Update stored amount to reflect actual delivery
            updated_items.append(updated_item)
            
            final_invoice_total += new_amount
        
        # Create invoice record
        invoice_no = f"INV-{po_id}-{int(datetime.now().timestamp())}"
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO invoices 
            (po_id, invoice_no, created_at, payment_mode, delivery_remarks, items_json, status, amount_received)
            VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)
        ''', (po_id, invoice_no, created_at, payment_mode, delivery_remarks, json.dumps(updated_items), amount_received))
        
        invoice_id = cursor.lastrowid
        
        # Update PO delivery status and ensure PACKAGED stage
        # We do NOT set delivered_at yet. That happens at actual delivery.
        # We set packaged_at to now.
        cursor.execute('''
            UPDATE purchase_orders 
            SET delivery_status = 'INVOICED', 
                delivery_stage = 'PACKAGED',
                packaged_at = ?, 
                items_json = ?, 
                amount_received = amount_received + ?,
                total_amount = ?
            WHERE id = ?
        ''', (created_at, json.dumps(updated_items), amount_received, final_invoice_total, po_id))
        
        conn.commit()
        
        print(f"[INFO] Invoice created: #{invoice_no} for PO #{po_id}")
        print(f"[INFO] PO #{po_id} delivery_status updated to INVOICED")
        
        conn.close()
        
        # Redirect to the generated invoice
        return RedirectResponse(url=f"/invoice/po/{po_id}", status_code=303)
        
    except Exception as e:
        conn.close()
        print(f"[ERROR] Invoice creation failed for PO #{po_id}: {e}")
        return JSONResponse(status_code=500, content={"error": "Failed to create invoice"})

# Amazon-Style Delivery Tracking Routes Management
@app.get("/admin/delivery_routes", response_class=HTMLResponse)
async def admin_delivery_routes(request: Request):
    require_admin(request)
    conn = get_db_connection()
    
    # Get all categories with hierarchical structure
    categories = conn.execute("""
        SELECT 
            cc.id, cc.name, cc.delivery_days, cc.route_name,
            cc.parent_id, cc.level,
            parent.name as parent_name,
            cc.is_active
        FROM customer_categories cc
        LEFT JOIN customer_categories parent ON cc.parent_id = parent.id
        ORDER BY cc.level, cc.name
    """).fetchall()
    
    conn.close()
    return templates.TemplateResponse("admin_delivery_routes.html", {
        "request": request,
        "categories": categories
    })

@app.post("/admin/delivery_routes/update")
async def update_delivery_route(request: Request):
    require_admin(request)
    form = await request.form()
    category_id = form.get("category_id")
    delivery_days = form.getlist("delivery_days")  # ["MONDAY", "FRIDAY"]
    route_name = form.get("route_name", "")
    
    conn = get_db_connection()
    
    # Update category
    if category_id and delivery_days:
        conn.execute("""
            UPDATE customer_categories
            SET delivery_days = ?, route_name = ?
            WHERE id = ?
        """, (json.dumps(delivery_days), route_name, category_id))
        
        # If updating parent, update all children
        if category_id and form.get("update_children") == "true":
            conn.execute("""
                UPDATE customer_categories
                SET parent_id = ?
                WHERE parent_id = ?
            """, (category_id, category_id))
    
    conn.commit()
    conn.close()
    
    return RedirectResponse(url="/admin/delivery_routes", status_code=303)

# Daily Background Job for Delivery Stage Updates
@app.on_event("startup")
async def startup_event():
    # Schedule daily task to update delivery stages
    asyncio.create_task(daily_delivery_stage_update())

async def daily_delivery_stage_update():
    while True:
        await asyncio.sleep(3600)  # Run every hour
        update_all_delivery_stages()

def update_all_delivery_stages():
    conn = get_db_connection()
    try:
        active_pos = conn.execute("""
            SELECT id, expected_delivery_date, delivery_stage, created_at
            FROM purchase_orders
            WHERE delivery_stage != 'DELIVERED' AND is_active = 1
        """).fetchall()
        
        for po in active_pos:
            update_delivery_timestamps(po['id'])
        
        conn.commit()
    finally:
        conn.close()

