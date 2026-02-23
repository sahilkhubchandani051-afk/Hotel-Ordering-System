// Notification System Logic

document.addEventListener('DOMContentLoaded', () => {
    initNotifications();
    if ('serviceWorker' in navigator) {
        initPushNotifications();
    }
});

function initNotifications() {
    const bellBtn = document.getElementById('notificationBtn');
    if (!bellBtn) return; // Not logged in

    // Poll for notifications
    fetchNotifications();
    setInterval(fetchNotifications, 60000); // Poll every minute

    // Toggle Drawer
    bellBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        const drawer = document.getElementById('notificationDrawer');
        drawer.classList.toggle('active');

        // Mark badge as read visually if needed, but usually strictly by API
    });

    // Close on click outside
    window.addEventListener('click', (e) => {
        const drawer = document.getElementById('notificationDrawer');
        const btn = document.getElementById('notificationBtn');
        if (drawer && drawer.classList.contains('active') && !drawer.contains(e.target) && !btn.contains(e.target)) {
            drawer.classList.remove('active');
        }
    });
}

function fetchNotifications() {
    fetch('/api/notifications')
        .then(res => {
            if (res.status === 401) return null; // Not logged in
            return res.json();
        })
        .then(data => {
            if (!data || !data.notifications) return;
            renderNotifications(data.notifications);
        })
        .catch(err => console.error('Notification fetch error:', err));
}

function renderNotifications(notifs) {
    const list = document.getElementById('notificationList');
    const badge = document.getElementById('notificationBadge');

    if (!list) return;

    // Count unread
    const unreadCount = notifs.filter(n => !n.is_read).length;

    // Update Badge
    if (unreadCount > 0) {
        badge.style.display = 'flex';
        badge.textContent = unreadCount > 9 ? '9+' : unreadCount;
    } else {
        badge.style.display = 'none';
    }

    // Render List
    list.innerHTML = '';

    if (notifs.length === 0) {
        list.innerHTML = '<div class="empty-notif">No new notifications</div>';
        return;
    }

    notifs.forEach(n => {
        const item = document.createElement('div');
        item.className = `notif-item ${n.is_read ? 'read' : 'unread'}`;
        item.onclick = () => handleNotificationClick(n);

        let timeStr = new Date(n.created_at).toLocaleString();

        item.innerHTML = `
            <div class="notif-content">
                <p class="notif-msg">${escapeHtml(n.message)}</p>
                <span class="notif-time">${timeStr}</span>
            </div>
            ${!n.is_read ? '<span class="blue-dot"></span>' : ''}
        `;
        list.appendChild(item);
    });
}

function handleNotificationClick(n) {
    // Mark as read
    fetch(`/api/notifications/${n.id}/read`, { method: 'POST' })
        .then(() => {
            // Navigate if link exists
            if (n.link) {
                window.location.href = n.link;
            } else {
                fetchNotifications(); // Refresh list to update UI
            }
        });
}

function escapeHtml(text) {
    return text
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

// --- Push Notifications ---
// --- Push Notifications ---
const VAPID_PUBLIC_KEY = 'BJmqMoqaCjYA4670ufEJVq3tN7uWLjDRjDGOJ47jz_bewcWQQ997YpGr3idWa4O1Myutvr9gnJHMQ1XZNR7J0tk';

function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding)
        .replace(/-/g, '+')
        .replace(/_/g, '/');
    const rawData = window.atob(base64);
    const outputArray = new Uint8Array(rawData.length);
    for (let i = 0; i < rawData.length; ++i) {
        outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
}

function initPushNotifications() {
    if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
        console.log("Push not supported");
        return;
    }

    // Register SW
    navigator.serviceWorker.register('/static/service-worker.js')
        .then(registration => {
            console.log('Service Worker registered');
            return registration;
        })
        .catch(err => console.error('SW registration failed:', err));

    // Check permission logic
    if (Notification.permission === 'default') {
        Notification.requestPermission().then(perm => {
            if (perm === 'granted') subscribeUserToPush();
        });
    } else if (Notification.permission === 'granted') {
        subscribeUserToPush();
    }
}

function subscribeUserToPush() {
    navigator.serviceWorker.ready.then(async (registration) => {
        try {
            const subscription = await registration.pushManager.subscribe({
                userVisibleOnly: true,
                applicationServerKey: urlBase64ToUint8Array(VAPID_PUBLIC_KEY)
            });

            console.log('User is subscribed:', subscription);

            // Send subscription to server
            await fetch('/api/push/subscribe', {
                method: 'POST',
                body: JSON.stringify(subscription),
                headers: {
                    'Content-Type': 'application/json'
                }
            });
        } catch (err) {
            console.error('Failed to subscribe the user: ', err);
        }
    });
}

// Optionally expose subscribe function globally so a button can call it
window.enablePushNotifications = () => {
    Notification.requestPermission().then(permission => {
        if (permission === 'granted') {
            subscribeUserToPush();
        }
    });
};
