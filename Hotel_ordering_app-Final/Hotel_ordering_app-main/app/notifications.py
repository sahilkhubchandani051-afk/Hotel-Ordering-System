import logging
import os
import httpx
import json
from datetime import datetime

logger = logging.getLogger(__name__)


def call_supabase_email_function(notification_id: int | None, recipient_email: str, subject: str, body: str) -> bool:
    """
    Makes a real HTTP POST request to Supabase Edge Function send-email-notifications.
    
    Args:
        notification_id: ID of the notification record from email_notifications table
        recipient_email: Email recipient
        subject: Email subject
        body: Email body
        
    Returns:
        bool: True if call succeeded, False otherwise
    """
    function_url = "https://bukvuodnprbjrjhwlnpx.supabase.co/functions/v1/send-email-notifications"

    try:
        SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")
        logger.info(
            "Calling Edge with anon key prefix: %s",
            SUPABASE_ANON_KEY[:10]
        )
        headers = {
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
            "apikey": SUPABASE_ANON_KEY,
            "Content-Type": "application/json",
        }
        payload = {"to": recipient_email, "subject": subject, "body": body}

        logger.info("Calling Supabase Edge Function send-email-notifications")
        with httpx.Client(timeout=10.0) as client:
            response = client.post(function_url, headers=headers, json=payload)
        logger.info(f"Supabase Edge response status: {response.status_code}")

        if notification_id is not None:
            try:
                from app.main import get_db_connection

                conn = get_db_connection()
                is_pg = os.getenv("DATABASE_URL") is not None
                if response.status_code == 200:
                    if is_pg:
                        conn.execute(
                            "UPDATE email_notifications SET status = 'SENT', sent_at = CURRENT_TIMESTAMP, error_message = NULL WHERE id = %s",
                            (notification_id,),
                        )
                    else:
                        conn.execute(
                            "UPDATE email_notifications SET status = 'SENT', sent_at = datetime('now'), error_message = NULL WHERE id = ?",
                            (notification_id,),
                        )
                else:
                    error_message = f"HTTP {response.status_code}: {response.text}"
                    if is_pg:
                        conn.execute(
                            "UPDATE email_notifications SET status = 'FAILED', error_message = %s WHERE id = %s",
                            (error_message, notification_id),
                        )
                    else:
                        conn.execute(
                            "UPDATE email_notifications SET status = 'FAILED', error_message = ? WHERE id = ?",
                            (error_message, notification_id),
                        )
                conn.commit()
                conn.close()
            except Exception as db_error:
                logger.error(f"Failed to update notification status: {db_error}")

        return response.status_code == 200

    except Exception as e:
        if notification_id is not None:
            try:
                from app.main import get_db_connection

                conn = get_db_connection()
                is_pg = os.getenv("DATABASE_URL") is not None
                if is_pg:
                    conn.execute(
                        "UPDATE email_notifications SET status = 'FAILED', error_message = %s WHERE id = %s",
                        (str(e), notification_id),
                    )
                else:
                    conn.execute(
                        "UPDATE email_notifications SET status = 'FAILED', error_message = ? WHERE id = ?",
                        (str(e), notification_id),
                    )
                conn.commit()
                conn.close()
            except Exception as db_error:
                logger.error(f"Failed to update notification status after error: {db_error}")
        return False


    pass


def send_push_notification(subscription_info: dict, message: str, url: str = "/") -> None:
    """Send a Web Push notification using pywebpush."""
    from app.main import VAPID_PRIVATE_KEY, VAPID_PUBLIC_KEY
    if not VAPID_PRIVATE_KEY or not subscription_info:
        return

    try:
        from pywebpush import webpush, WebPushException
        
        payload = json.dumps({
            "title": "HotelSys",
            "body": message,
            "url": url
        })
        
        webpush(
            subscription_info=subscription_info,
            data=payload,
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub": "mailto:admin@hotel.com"},
            ttl=60
        )
        logger.info("Push notification sent successfully")
    except Exception as e:
        logger.error(f"Push notification failed: {e}")


def send_email_notification(
    event_type: str,
    ref_id: int,
    recipient_role: str,
    recipient_email: str,
    subject: str,
    message: str,
    send_email: bool = False,
) -> int:
    """Logs an email notification and immediately calls Supabase Edge Function.

    Returns notification_id if successful, None if failed.
    Failures must never break main operations; this function never raises.
    """
    notification_id: int | None = None

    # 1) Log into DB (best-effort) - NON-BLOCKING
    try:
        from app.main import get_db_connection

        is_pg = os.getenv("DATABASE_URL") is not None
        conn = get_db_connection()
        try:
            if is_pg:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO email_notifications (event_type, ref_id, recipient_role, recipient_email, status)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (event_type, ref_id, recipient_role, recipient_email, "PENDING"),
                )
                result = cursor.fetchone()
                notification_id = result[0] if result else None
            else:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO email_notifications (event_type, ref_id, recipient_role, recipient_email, status)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (event_type, ref_id, recipient_role, recipient_email, "PENDING"),
                )
                notification_id = cursor.lastrowid
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.warning(f"Failed to log {event_type} notification: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Failed to log {event_type} notification: {e}")

    # 2) ALWAYS call Supabase Edge Function (must not depend on DB insert)
    # TASK 1: ENSURE TABLE STATUS UPDATE HAPPENS
    try:
        # Store the result to ensure status update logic runs
        email_result = call_supabase_email_function(
            notification_id=notification_id,
            recipient_email=recipient_email,
            subject=subject,
            body=message,
        )
        logger.info(f"Email call result for {event_type}: {email_result}")
    except Exception as e:
        logger.error(f"Failed to call Supabase Edge Function for {event_type}: {e}")

    return notification_id


def dispatch_pending_notifications() -> None:
    try:
        pass
    except Exception:
        # Must never raise
        pass
