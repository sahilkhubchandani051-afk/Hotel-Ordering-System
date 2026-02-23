
# --- Customer Categories Management ---

@app.get("/admin/customer_categories", response_class=HTMLResponse)
async def admin_customer_categories(request: Request):
    require_admin(request)
    conn = get_db_connection()
    try:
        categories = conn.execute("SELECT * FROM customer_categories ORDER BY name").fetchall()
    except Exception:
        # Table might not exist yet
        categories = []
    conn.close()
    
    return templates.TemplateResponse("admin_customer_categories.html", {
        "request": request, 
        "categories": categories
    })

@app.post("/admin/customer_categories/add")
async def add_customer_category(request: Request, category_name: str = Form(...)):
    require_admin(request)
    name = category_name.strip()
    if name:
        conn = get_db_connection()
        try:
             # Postgres vs Sqlite syntax handled by driver usually for simple insert
             if DATABASE_URL:
                 conn.execute("INSERT INTO customer_categories (name) VALUES (%s)", (name,))
             else:
                 conn.execute("INSERT INTO customer_categories (name) VALUES (?)", (name,))
             conn.commit()
        except Exception as e:
            print(f"Error adding customer category: {e}")
        conn.close()
        
    return RedirectResponse(url="/admin/customer_categories", status_code=303)

@app.post("/admin/customer_categories/update")
async def update_customer_category(request: Request, category_id: int = Form(...), category_name: str = Form(...)):
    require_admin(request)
    name = category_name.strip()
    if name:
        conn = get_db_connection()
        try:
             if DATABASE_URL:
                 conn.execute("UPDATE customer_categories SET name = %s WHERE id = %s", (name, category_id))
             else:
                 conn.execute("UPDATE customer_categories SET name = ? WHERE id = ?", (name, category_id))
             conn.commit()
        except Exception as e:
            print(f"Error updating customer category: {e}")
        conn.close()
    return RedirectResponse(url="/admin/customer_categories", status_code=303)

@app.post("/admin/customer_categories/delete")
async def delete_customer_category(request: Request, category_name: str = Form(...)):
    require_admin(request)
    conn = get_db_connection()
    try:
        if DATABASE_URL:
             conn.execute("DELETE FROM customer_categories WHERE name = %s", (category_name,))
        else:
             conn.execute("DELETE FROM customer_categories WHERE name = ?", (category_name,))
        conn.commit()
    except Exception as e:
        print(f"Error deleting customer category: {e}")
    conn.close()
    return RedirectResponse(url="/admin/customer_categories", status_code=303)
