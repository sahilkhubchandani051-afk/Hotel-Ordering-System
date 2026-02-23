"""Seed a test customer + PENDING_PRICE_REVIEW PR for verification."""
import sqlite3
import json
import hashlib
from datetime import datetime

DB_PATH = "orders.db"
print(f"Using DB: {DB_PATH}")
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# 1. Insert test customer — password hash same as what the app uses
# Check the app's login route to match the hash type (usually plain text or md5)
pwd = hashlib.md5("test1234".encode()).hexdigest()
try:
    conn.execute("""
        INSERT OR IGNORE INTO customers (email, password, name, full_name, business_name, mobile, address, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, ("testhotel@test.com", pwd, "Test Hotel", "Test Hotel Owner",
          "Test Hotel Ltd", "9876543210", "123 Test Street", "Active"))
    conn.commit()
    print("Customer seeded OK (testhotel@test.com)")
except Exception as e:
    print(f"Customer insert: {e}")

# 2. Get real products from the DB
prods = conn.execute("SELECT id, name, unit, rate FROM products LIMIT 3").fetchall()
if not prods:
    print("No products in DB. Inserting test products.")
    conn.execute("INSERT INTO products (name, unit, rate) VALUES ('Rice', 'kg', 80.0)")
    conn.execute("INSERT INTO products (name, unit, rate) VALUES ('Sugar', 'kg', 50.0)")
    conn.commit()
    prods = conn.execute("SELECT id, name, unit, rate FROM products LIMIT 3").fetchall()

items = []
for p in prods[:2]:
    items.append({
        "product_id": p["id"],
        "name": p["name"],
        "unit": p["unit"],
        "qty": 3,
        "quoted_rate": 0,
    })
print(f"Using products: {[(p['id'], p['name']) for p in prods[:2]]}")

# 3. Insert PENDING_PRICE_REVIEW PR
created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
conn.execute("""
    INSERT INTO purchase_requisitions (customer_email, status, created_at, items_json, workflow_type)
    VALUES (?, ?, ?, ?, ?)
""", ("testhotel@test.com", "PENDING_PRICE_REVIEW", created_at, json.dumps(items), "ADMIN_PRICE_REVIEW"))
conn.commit()
pr_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
print(f"PR #{pr_id} inserted with status=PENDING_PRICE_REVIEW")
print()
print(f"Admin Price Review URL: http://localhost:8000/admin/price_review/{pr_id}")
print(f"PR List Filter URL:     http://localhost:8000/admin/pr_list?status=PENDING_PRICE_REVIEW")
conn.close()
