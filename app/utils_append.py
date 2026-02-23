
# -------------------------------------------------------------------------
# Delivery Scheduling Utils
# -------------------------------------------------------------------------

def calculate_next_delivery_date(order_datetime: datetime, delivery_days: list, cutoff_hour: int = 17) -> str:
    """
    Calculate the expected delivery date based on order time and allowed delivery days.
    
    Args:
        order_datetime (datetime): The time the order was placed.
        delivery_days (list): List of allowed days (e.g. ["Monday", "Wednesday"]).
                              If empty, assumes daily delivery.
        cutoff_hour (int): Hour after which order is processed next day (default 5 PM).
        
    Returns:
        str: YYYY-MM-DD
    """
    # 1. Start checking from Today (if before cutoff) or Tomorrow
    current_check_date = order_datetime.date()
    
    if order_datetime.hour >= cutoff_hour:
        current_check_date += timedelta(days=1)
        
    # If no specific days restricted, just return the calculated date (next valid business day)
    # Default: Next day delivery if before cutoff, else day after
    if not delivery_days:
        # If it was after cutoff, we already moved to tomorrow.
        # If it was before cutoff, standard is usually next day delivery anyway for B2B?
        # Let's assume standard B2B is T+1 (Next Day).
        
        # If order < Cutoff: Deliver Tomorrow.
        # If order >= Cutoff: Deliver Day After Tomorrow.
        
        # My logic above:
        # If >= cutoff, current_check_date is Tomorrow.
        # If < cutoff, current_check_date is Today.
        
        # Deliver T+1 always?
        delivery_date = current_check_date + timedelta(days=1)
        return delivery_date.strftime("%Y-%m-%d")

    # If restricted days exist
    # Map day names to weekday integers (Monday=0, Sunday=6)
    day_map = {
        "Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, 
        "Friday": 4, "Saturday": 5, "Sunday": 6
    }
    
    allowed_indices = [day_map[d] for d in delivery_days if d in day_map]
    
    if not allowed_indices:
        # Fallback if parsing failed
        return (current_check_date + timedelta(days=1)).strftime("%Y-%m-%d")
        
    # Start checking from T+1 (Earliest delivery is tomorrow, never today unless explicit same-day support which is rare in this context)
    candidate_date = current_check_date + timedelta(days=1)
    
    # Try next 14 days to find a slot
    for _ in range(14):
        if candidate_date.weekday() in allowed_indices:
            return candidate_date.strftime("%Y-%m-%d")
        candidate_date += timedelta(days=1)
        
    # Fallback
    return candidate_date.strftime("%Y-%m-%d")


def is_quotation_expired(created_at_str: str, validity_days: int = 7) -> bool:
    """
    Check if a quotation is expired (older than validity_days).
    """
    if not created_at_str:
        return False
        
    try:
        # Handle various formats
        if isinstance(created_at_str, str):
            if 'T' in created_at_str:
                dt = datetime.fromisoformat(created_at_str)
            else:
                dt = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
        elif isinstance(created_at_str, datetime):
            dt = created_at_str
        else:
            return False
            
        expires_on = dt + timedelta(days=validity_days)
        return datetime.now() > expires_on
    except Exception:
        return False
