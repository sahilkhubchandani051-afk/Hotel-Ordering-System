
import random
import string
import smtplib
import logging
import requests
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

# Simple logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OTP Functions Removed (Authenication migrated to Supabase) 
# Restored for Password Reset Flow
def generate_otp() -> str:
    """Generate a 6-digit OTP."""
    import random
    return str(random.randint(100000, 999999))

def number_to_words(amount):
    """
    Convert a numeric amount to English words (Indian Format).
    """
    try:
        amount = float(amount)
        if amount == 0:
            return "Zero Rupees"
            
        def num_to_words_upto_999(n):
            units = ["", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine"]
            teens = ["Ten", "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen", "Sixteen", "Seventeen", "Eighteen", "Nineteen"]
            tens = ["", "", "Twenty", "Thirty", "Forty", "Fifty", "Sixty", "Seventy", "Eighty", "Ninety"]
            
            words = []
            
            h = n // 100
            t = (n % 100)
            
            if h > 0:
                words.append(units[h] + " Hundred")
                
            if t > 0:
                if t < 10:
                    words.append(units[t])
                elif t < 20:
                    words.append(teens[t-10])
                else:
                    words.append(tens[t//10])
                    if t % 10 > 0:
                        words.append(units[t%10])
                        
            return " ".join(words)

        def convert_whole_number(n):
            if n == 0: return ""
            
            parts = []
            
            # Crores
            crore = n // 10000000
            n = n % 10000000
            if crore > 0:
                parts.append(convert_whole_number(crore) + " Crore")
            
            # Lakhs
            lakh = n // 100000
            n = n % 100000
            if lakh > 0:
                parts.append(convert_whole_number(lakh) + " Lakh")
            
            # Thousands
            thousand = n // 1000
            n = n % 1000
            if thousand > 0:
                parts.append(num_to_words_upto_999(thousand) + " Thousand")
                
            if n > 0:
                parts.append(num_to_words_upto_999(n))
                
            return " ".join(parts)

        # Split Rupees and Paise
        rupees = int(amount)
        paise = int(round((amount - rupees) * 100))
        
        rupees_words = convert_whole_number(rupees)
        
        result = [rupees_words, "Rupees"]
        
        if paise > 0:
            paise_words = num_to_words_upto_999(paise)
            result.extend(["and", paise_words, "Paise"])
            
        return " ".join(result) + " Only"
        
    except Exception as e:
        return f"{amount} (in words error)"

def translate_text(text: str, target_lang: str, source_lang: str = None) -> str:
    """
    Translate text using the Google Translate REST API.
    Fail-safe: Returns original text if API key is missing or request fails.
    """
    api_key = os.getenv("GOOGLE_TRANSLATE_API_KEY")
    if not api_key:
        return text

    if not text or not text.strip():
        return text

    url = "https://translation.googleapis.com/language/translate/v2"
    params = {
        "q": text,
        "target": target_lang,
        "key": api_key
    }
    if source_lang:
        params["source"] = source_lang

    try:
        response = requests.get(url, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            translations = data.get("data", {}).get("translations", [])
            if translations:
                return translations[0].get("translatedText", text)
        else:
            logger.warning(f"Translation API error: {response.status_code} - {response.text}")
    except Exception as e:
        logger.warning(f"Translation request failed: {e}")

    return text

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
        delivery_date = current_check_date + timedelta(days=1)
        return delivery_date.strftime("%Y-%m-%d")

    # If restricted days exist
    day_map = {
        "Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, 
        "Friday": 4, "Saturday": 5, "Sunday": 6
    }
    
    allowed_indices = [day_map[d] for d in delivery_days if d in day_map]
    
    if not allowed_indices:
        return (current_check_date + timedelta(days=1)).strftime("%Y-%m-%d")
        
    candidate_date = current_check_date
    
    for _ in range(14):
        if candidate_date.weekday() in allowed_indices:
            return candidate_date.strftime("%Y-%m-%d")
        candidate_date += timedelta(days=1)
        
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
