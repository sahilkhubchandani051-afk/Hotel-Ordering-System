from fastapi import APIRouter, Request, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
# Imports moved inside functions to avoid circular dependency
from app.services.inventory_service import InventoryService
from pydantic import BaseModel
import json
from datetime import datetime, timedelta
from typing import List, Optional


router = APIRouter()

@router.get("/admin/inventory/ledger", response_class=HTMLResponse)
async def view_ledger_select(request: Request):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    
    # Fetch products with categories
    query = """
        SELECT p.id, p.name, p.unit, c.name as category
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        ORDER BY p.name
    """
    products = conn.execute(query).fetchall()
    conn.close()
    
    return templates.TemplateResponse("admin_inventory_ledger_select.html", {
        "request": request,
        "products": products
    })


# Data Models
class BulkItem(BaseModel):
    product_id: int
    qty: float
    location_id: int # 1=Godown, 2=Shop

class BulkReceiveRequest(BaseModel):
    items: List[BulkItem]
    reference_type: str # 'OPENING', 'PURCHASE'
    reference_id: str
    transaction_type: str = 'RECEIVE'
    date: Optional[str] = None
    remark: Optional[str] = None

class TransferRequest(BaseModel):
    product_id: int
    qty: float
    from_loc: int
    to_loc: int
    remark: str

class BulkTransferItem(BaseModel):
    product_id: int
    qty: float
    # Locations are global for the batch usually, or per item. 
    # Requirement: "Transfer Stock (Godown -> Shop)" implies fixed direction.
    # So item just needs product and qty.

class BulkTransferRequest(BaseModel):
    items: List[BulkTransferItem]
    from_loc: int # 1
    to_loc: int   # 2
    date: Optional[str] = None
    remark: Optional[str] = None

class ReorderUpdate(BaseModel):
    product_id: int
    reorder_shop: int
    reorder_godown: int

class BulkReorderItem(BaseModel):
    product_id: int
    reorder_shop: int
    reorder_godown: int

class BulkReorderRequest(BaseModel):
    items: List[BulkReorderItem]

# -----------------------------------------------------------------------------
# Views (Pages)
# -----------------------------------------------------------------------------

@router.get("/admin/inventory", response_class=HTMLResponse)
async def inventory_dashboard_page(request: Request):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    return templates.TemplateResponse("admin_inventory_dashboard.html", {"request": request})

@router.get("/admin/inventory/bulk", response_class=HTMLResponse)
async def bulk_entry_page(request: Request):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    # Pass products for the UI to build the dropdown
    conn = get_db_connection()
    products = conn.execute("SELECT id, name, unit, category_id FROM products ORDER BY name").fetchall()
    
    # Locations
    locations = [{"id": 1, "name": "GODOWN"}, {"id": 2, "name": "SHOP"}]
    
    conn.close()
    return templates.TemplateResponse("admin_inventory_bulk_entry.html", {
        "request": request,
        "products": [dict(p) for p in products],
        "locations": locations
    })

@router.get("/admin/inventory/reorder", response_class=HTMLResponse)
async def bulk_reorder_page(request: Request):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    # Fetch all products with current reorder levels
    products = conn.execute("SELECT id, name, unit, reorder_level_shop, reorder_level_godown FROM products ORDER BY name").fetchall()
    conn.close()
    return templates.TemplateResponse("admin_inventory_reorder.html", {
        "request": request,
        "products": [dict(p) for p in products]
    })

@router.get("/admin/inventory/transfer", response_class=HTMLResponse)
async def transfer_page(request: Request):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    service = InventoryService(conn)
    # Get products with current stock summary to show available qty
    summary = service.get_stock_summary()
    conn.close()
    return templates.TemplateResponse("admin_inventory_transfer.html", {
        "request": request,
        "products": summary # Contains id, name, unit, godown_stock, shop_stock
    })

@router.get("/admin/inventory/ledger/{product_id}", response_class=HTMLResponse)
async def view_ledger_page(request: Request, product_id: int):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    try:
        prod = conn.execute("SELECT name, unit FROM products WHERE id = ?", (product_id,)).fetchone()
        product_name = prod['name'] if prod else "Unknown Product"
        product_unit = prod['unit'] if prod else ""
        
        return templates.TemplateResponse("admin_inventory_ledger.html", {
            "request": request,
            "product_id": product_id,
            "product_name": product_name,
            "product_unit": product_unit
        })
    finally:
        conn.close()

@router.post("/admin/inventory/bulk-receive")
async def bulk_receive_stock(request: Request, payload: BulkReceiveRequest):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    service = InventoryService(conn)
    
    try:
        count = 0
        
        if payload.transaction_type == 'OPENING_STOCK':
             for item in payload.items:
                if item.qty > 0:
                    if not service.is_ledger_empty(item.product_id, item.location_id):
                        return JSONResponse(status_code=400, content={
                            "error": f"Product ID {item.product_id} already has transaction history at Location {item.location_id}. Cannot set Opening Stock."
                        })

        # 2. Process Items
        for item in payload.items:
            if item.qty > 0:
                service.receive_stock(
                    product_id=item.product_id, 
                    qty=item.qty, 
                    location_id=item.location_id, 
                    reference_type=payload.reference_type,
                    reference_id=payload.remark if payload.remark else payload.reference_id, # Use remark as ref_id if present
                    transaction_type=payload.transaction_type,
                    created_at=payload.date
                )
                count += 1
        if count > 0:
            conn.commit()
            
        return JSONResponse({"success": True, "message": f"Processed {count} items"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        conn.close()

@router.post("/admin/inventory/transfer")
async def transfer_stock_api(request: Request, payload: TransferRequest):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    service = InventoryService(conn)
    
    try:
        service.transfer_stock(
            product_id=payload.product_id,
            qty=payload.qty,
            from_loc=payload.from_loc,
            to_loc=payload.to_loc,
            reference_id=payload.remark
        )
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    finally:
        conn.close()

@router.post("/admin/inventory/bulk-transfer")
async def bulk_transfer_stock_api(request: Request, payload: BulkTransferRequest):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    service = InventoryService(conn)
    
    try:
        count = 0
        errors = []
        # Atomic transaction would be ideal, but for now we process row by row
        # If one fails, others might succeed. 
        # But for "Bulk Submit", typically we want all or nothing or at least reporting.
        # InventoryService doesn't have bulk_transfer transaction support yet (single commit per call).
        # However, `transfer_stock` commits internally.
        # We should ideally refactor to control commit, but given constraints, we'll try/except each.
        
        for item in payload.items:
            if item.qty > 0:
                try:
                    service.transfer_stock(
                        product_id=item.product_id,
                        qty=item.qty,
                        from_loc=payload.from_loc,
                        to_loc=payload.to_loc,
                        reference_id=payload.remark if payload.remark else "Bulk Transfer",
                        created_at=payload.date
                    )
                    count += 1
                except ValueError as ve:
                    # Stock insufficient
                    errors.append(f"Product {item.product_id}: {str(ve)}")
                except Exception as e:
                    errors.append(f"Product {item.product_id}: {str(e)}")
                    
        if errors:
            return JSONResponse({"success": False, "message": f"Processed {count} items. Errors: {'; '.join(errors)}"})
            
        return JSONResponse({"success": True, "message": f"Transferred {count} items successfully"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        conn.close()

@router.get("/api/inventory/ledger/{product_id}")
async def get_ledger_api(request: Request, product_id: int):
    conn = None
    conn = None
    try:
        from app.main import get_db_connection, templates, require_admin
        require_admin(request)
        conn = get_db_connection()
        service = InventoryService(conn)
        
        start_date = request.query_params.get('start_date')
        end_date = request.query_params.get('end_date')
        if start_date == "": start_date = None
        if end_date == "": end_date = None
        
        location = request.query_params.get('location')
        if location == "": location = None
        
        trans_type = request.query_params.get('type')
        if trans_type == "": trans_type = None

        ledger = service.get_ledger(
            product_id, 
            start_date=start_date, 
            end_date=end_date, 
            location_id=int(location) if location else None, 
            transaction_type=trans_type
        )
        
        # Get Current Balances
        cursor = service.get_cursor()
        godown = service._calculate_running_balance(cursor, product_id, 1)
        shop = service._calculate_running_balance(cursor, product_id, 2)
        prod = conn.execute("SELECT name, unit FROM products WHERE id = ?", (product_id,)).fetchone()
        
        entries = []
        for l in ledger:
            entries.append({
                "date": l['created_at'],
                "location": l['location_name'],
                "type": l['transaction_type'],
                "qty_change": float(l['qty_change']),
                "balance": float(l['running_balance']),
                "remark": f"{l['reference_type']} {l.get('reference_id','')}"
            })

        return JSONResponse({
            "product_name": prod['name'] if prod else "Unknown",
            "unit": prod['unit'] if prod else "",
            "current_stock": shop, # Display Shop as primary in header
            "godown_stock": godown,
            "entries": entries
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()

@router.get("/admin/inventory/dashboard/data")
async def get_dashboard_data(request: Request):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    service = InventoryService(conn)
    
    try:
        # Get summarized stock (Godown, Shop)
        summary = service.get_stock_summary()
        
        total_products = len(summary)
        low_stock_count = 0 # Shop Low
        transfer_needed_count = 0
        purchase_needed_count = 0
        
        # Calculate Alerts and Actions
        for p in summary:
            # Shop Logic
            s_reorder = p.get('reorder_level_shop')
            s_threshold = s_reorder if s_reorder is not None else 5
            
            # Ensure stock is float for JSON serialization (Postgres returns Decimal)
            p['shop_stock'] = float(p['shop_stock'] or 0)
            p['godown_stock'] = float(p['godown_stock'] or 0)
            
            shop_low = p['shop_stock'] <= s_threshold
            
            # Godown Logic
            g_reorder = p.get('reorder_level_godown')
            g_threshold = g_reorder if g_reorder is not None else 10
            godown_low = p['godown_stock'] <= g_threshold
            godown_has_stock = p['godown_stock'] > 0

            # Determine Action
            action = "OK"
            suggested_transfer_qty = 0
            
            if shop_low:
                low_stock_count += 1
                if godown_has_stock:
                    action = "TRANSFER"
                    transfer_needed_count += 1
                    # Suggest diff to reach threshold, capped by godown stock
                    needed = s_threshold - p['shop_stock']
                    if needed <= 0: needed = 5 # Fallback default
                    suggested_transfer_qty = min(needed, p['godown_stock'])
                else:
                    action = "PURCHASE"
                    purchase_needed_count += 1
            elif godown_low:
                action = "MONITOR"
            
            # Sales Intelligence (7d and 30d)
            # We'll calculate this by querying the purchase_orders items_json
            # Note: For performance in very large datasets, this should be pre-aggregated, 
            # but for this scale, we'll calculate it on the fly from the invoices/po history.
            
            # Fetch quantities from last 7 and 30 days for this product
            # delivery_status='INVOICED' identifies finalized sales
            # Fetch all invoiced orders from last 30 days
            # We process parsing in Python to ensure compatibility between SQLite (json_extract) and Postgres (json operands)
            cutoff_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
            sales_rows = conn.execute("""
                SELECT created_at, items_json 
                FROM purchase_orders 
                WHERE delivery_status = 'INVOICED' 
                  AND created_at >= ?
            """, (cutoff_date,)).fetchall()

            qty_7d = 0.0
            qty_30d = 0.0
            
            now = datetime.now()
            date_7d = now - timedelta(days=7)
            
            for row in sales_rows:
                # Parse date
                try:
                    # Handle potential different date formats if needed, but usually ISO
                    po_date_str = row['created_at']
                    # Simple check: created_at is usually "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD"
                    # We can compare string directly if ISO, or parse. Let's parse to be safe.
                    if len(po_date_str) > 19: po_date_str = po_date_str[:19]
                    po_date = datetime.strptime(po_date_str.replace("T", " "), "%Y-%m-%d %H:%M:%S")
                except:
                    # Fallback
                    continue

                # Parse JSON
                try:
                    items = json.loads(row['items_json'])
                    for item in items:
                        if str(item.get('id')) == str(p['id']):
                            qty = float(item.get('qty', 0))
                            
                            # Add to 30d (since query filtered to >= 30d)
                            qty_30d += qty
                            
                            # Add to 7d if recent
                            if po_date >= date_7d:
                                qty_7d += qty
                except:
                    continue

            avg_daily_7d = qty_7d / 7.0
            avg_daily_30d = qty_30d / 30.0
            
            # Classification
            velocity = "Slow"
            if avg_daily_30d >= 1.0: velocity = "Fast"
            elif avg_daily_30d >= 0.1: velocity = "Normal"
            
            # Coverage
            coverage_days = 999
            if avg_daily_30d > 0:
                coverage_days = int(p['shop_stock'] / avg_daily_30d)

            p['intelligence'] = {
                "avg_daily_7d": round(avg_daily_7d, 2),
                "avg_daily_30d": round(avg_daily_30d, 2),
                "velocity": velocity,
                "coverage_days": coverage_days
            }

            p['suggested_action'] = action
            p['suggested_transfer_qty'] = float(suggested_transfer_qty)
                
        result = {
            "total_products": total_products,
            "low_stock_count": low_stock_count,
            "transfer_needed_count": transfer_needed_count,
            "purchase_needed_count": purchase_needed_count, 
            "products": summary
        }
        print(f"DASHBOARD_API_KEYS: {list(result.keys())}")
        return JSONResponse(result)
    finally:
        conn.close()

@router.post("/admin/inventory/update-reorder")
async def update_reorder_level(request: Request, payload: ReorderUpdate):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    try:
        conn.execute("UPDATE products SET reorder_level_shop = ?, reorder_level_godown = ? WHERE id = ?", 
                     (payload.reorder_shop, payload.reorder_godown, payload.product_id))
        conn.commit()
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        conn.close()

@router.post("/admin/inventory/bulk-reorder-update")
async def bulk_update_reorder_api(request: Request, payload: BulkReorderRequest):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    try:
        # Use transaction
        cursor = conn.cursor()
        for item in payload.items:
             cursor.execute(
                 "UPDATE products SET reorder_level_shop = ?, reorder_level_godown = ? WHERE id = ?",
                 (item.reorder_shop, item.reorder_godown, item.product_id)
             )
        conn.commit()
        return JSONResponse({"success": True, "count": len(payload.items)})
    except Exception as e:
        conn.rollback()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        conn.close()

# -----------------------------------------------------------------------------
# Stock Adjustment Routes
# -----------------------------------------------------------------------------

@router.get("/admin/inventory/adjust", response_class=HTMLResponse)
async def adjustment_page(request: Request):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    
    # Products
    products = conn.execute("SELECT id, name, unit FROM products ORDER BY name").fetchall()
    conn.close()
    
    return templates.TemplateResponse("admin_inventory_adjustment.html", {
        "request": request,
        "products": [dict(p) for p in products]
    })

class AdjustmentRequest(BaseModel):
    product_id: int
    location_id: int
    adjustment_type: str # 'INCREASE' or 'DECREASE'
    qty: float
    reason: str
    remark: Optional[str] = None

@router.post("/admin/inventory/adjust")
async def process_adjustment(request: Request, payload: AdjustmentRequest):
    from app.main import get_db_connection, templates, require_admin
    require_admin(request)
    conn = get_db_connection()
    service = InventoryService(conn)
    
    try:
        result = service.adjust_stock(
            product_id=payload.product_id,
            location_id=payload.location_id,
            qty=payload.qty,
            adjustment_type=payload.adjustment_type,
            reason=payload.reason,
            remark=payload.remark
        )
        return JSONResponse(result)
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        conn.close()
