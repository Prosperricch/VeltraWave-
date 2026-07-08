import hashlib
import hmac
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager

import requests
from flask import Flask, render_template, request, jsonify
import json

# Load a local .env file when present (e.g. running on your machine). On
# Render the real environment variables you set in the dashboard are used
# directly, so this is a no-op there — it's purely a local-dev convenience.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = Flask(__name__)

email = "prosper@email.com"

# ============================================================================
# Secrets / config — pulled from environment variables (set these in Render's
# "Environment" tab for the service). Never hardcode real tokens in source.
#
# PAYMENT_TOKEN  — Paystack secret key
# PAYMENT_URL    — Paystack initialize-transaction endpoint
# PROVIDER_TOKEN — n3tdata API token (already includes the "Token " prefix
#                  n3tdata expects, e.g. "Token abcd1234...")
# ============================================================================
PAYMENT_TOKEN = os.environ.get("PAYMENT_TOKEN")
PAYMENT_URL = os.environ.get("PAYMENT_URL", "https://api.paystack.co/transaction/initialize")
PROVIDER_TOKEN = os.environ.get("PROVIDER_TOKEN")

# n3tdata endpoints aren't secrets, so they stay as plain constants. Move
# these to env vars too if you ever need to point at a staging provider.
PROVIDER_URL = os.environ.get("PROVIDER_URL")
PROVIDER_URL_AIRTIME = os.environ.get("PROVIDER_URL_AIRTIME")
_missing_env = [
    name for name, value in {
        "PAYMENT_TOKEN": PAYMENT_TOKEN,
        "PAYMENT_URL": PAYMENT_URL,
        "PROVIDER_TOKEN": PROVIDER_TOKEN,
    }.items() if not value
]
if _missing_env:
    raise RuntimeError(
        "Missing required environment variable(s): " + ", ".join(_missing_env) +
        ". Set these in Render's Environment tab (or a local .env file) before starting the app."
    )

# ============================================================================
# Keep-alive (avoid Render free-tier "spin down after inactivity")
#
# Render's free web services sleep after ~15 minutes without an incoming
# HTTP request. This background thread pings our own /healthz endpoint
# every KEEP_ALIVE_INTERVAL_SECONDS so the service always looks "active".
#
# Notes / caveats (read before relying on this):
#  - RENDER_EXTERNAL_URL is set automatically by Render for every web
#    service, so this is a no-op locally (nothing to ping) and safe to
#    leave in the code.
#  - If you run gunicorn with more than 1 worker, each worker process will
#    start its own keep-alive thread, so you'll get N pings per interval
#    instead of 1. That's harmless (just a few extra requests) but if you
#    want it perfectly clean, either run a single worker
#    (`gunicorn app:app --workers 1`) or use an external uptime monitor
#    instead (see below).
#  - This trick does NOT bypass Render's policy — it just means the
#    service genuinely receives regular traffic. Render can still change
#    this behavior at any time. A more robust, zero-maintenance
#    alternative is a free external uptime pinger such as
#    https://cron-job.org or https://uptimerobot.com hitting
#    https://<your-app>.onrender.com/healthz every 10 minutes — that also
#    works even if you scale to multiple instances.
# ============================================================================
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL")
KEEP_ALIVE_INTERVAL_SECONDS = int(os.environ.get("KEEP_ALIVE_INTERVAL_SECONDS", 600))  # 10 min


def _keep_alive_loop():
    if not RENDER_EXTERNAL_URL:
        print("⏭️  Keep-alive disabled (RENDER_EXTERNAL_URL not set — not running on Render).")
        return

    ping_url = RENDER_EXTERNAL_URL.rstrip("/") + "/healthz"
    # Small initial delay so the server has fully booted before the first ping.
    time.sleep(30)
    while True:
        try:
            resp = requests.get(ping_url, timeout=10)
            print(f"💓 Keep-alive ping → {ping_url} ({resp.status_code})")
        except Exception as e:
            print(f"⚠️  Keep-alive ping failed: {e}")
        time.sleep(KEEP_ALIVE_INTERVAL_SECONDS)


def _start_keep_alive_thread():
    thread = threading.Thread(target=_keep_alive_loop, daemon=True)
    thread.start()


# Start it once at import time (works whether launched via `python app.py`
# or via `gunicorn app:app`).
_start_keep_alive_thread()


@app.route("/healthz", methods=["GET"])
def healthz():
    """Lightweight health/keep-alive endpoint. No side effects, no auth."""
    return {"status": "ok"}, 200


# ============================================================================
# Airtime discount config
#
# Customers pay AIRTIME_DISCOUNT_PERCENT less than the airtime value they
# actually receive — the discount is absorbed on our end (we still send the
# FULL requested amount to the provider), not passed on to n3tdata.
#
# This is always computed server-side. Never trust a "pay_amount"/"discount"
# value sent from the client — the frontend shows a live preview using the
# same formula, but this function is the real source of truth.
# ============================================================================
AIRTIME_DISCOUNT_PERCENT = 2
AIRTIME_MIN_AMOUNT = 50
AIRTIME_MAX_AMOUNT = 50000


def _apply_airtime_discount(airtime_amount: int) -> int:
    """Given the airtime value the customer wants delivered, return what
    they should actually be charged after the discount."""
    discount = (airtime_amount * AIRTIME_DISCOUNT_PERCENT) / 100
    pay_amount = round(airtime_amount - discount)
    return max(pay_amount, 1)


# ============================================================================
# Persistent order tracker, keyed by Paystack reference.
#
# IMPORTANT FIX: this used to be a plain in-memory dict (`ORDERS = {}`).
# That silently breaks in production because gunicorn can run more than
# one worker *process*, and each process gets its OWN copy of that dict.
# What was actually happening:
#   1. Browser POSTs /purchase_data -> handled by (say) worker A, which
#      creates the order in worker A's in-memory ORDERS dict.
#   2. Paystack's charge.success webhook POSTs /payment-webhooks -> load
#      balanced to whichever worker happens to be free (maybe worker B),
#      which has no idea that reference exists, so the "order is None"
#      branch fires and nothing gets updated anywhere the customer can see.
#   3. The ticket page polls GET /api/order-status/<ref> -> handled by
#      worker A, C, or whoever's free -> still shows "pending" forever,
#      because the success/failure was only ever written into worker B's
#      private memory.
# This is exactly the "still processing / never resolves" symptom.
#
# Fix: back the order store with a small SQLite file on disk. SQLite file
# access is shared by every worker process on the same instance (unlike
# a Python dict in RAM), so no matter which worker handles which request,
# they all read/write the same underlying data. WAL mode keeps concurrent
# reads/writes from a handful of workers safe and fast.
#
# Note: Render's free-tier disk is still ephemeral across deploys/restarts
# (a fresh deploy wipes it), so this doesn't give you permanent history —
# but it DOES fix the cross-worker bug, which is the actual issue here. If
# you need orders to survive redeploys long-term, swap this for Postgres/
# Redis later; the order_get/order_set/order_update functions below are the
# only place you'd need to change.
# ============================================================================
DB_PATH = os.environ.get(
    "ORDERS_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "orders.db")
)


@contextmanager
def _db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _init_orders_db():
    with _db_conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                reference   TEXT PRIMARY KEY,
                data        TEXT NOT NULL,
                created_at  REAL NOT NULL
            )
        """)


def order_set(reference, order):
    """Create or fully overwrite the stored order for this reference."""
    with _db_conn() as conn:
        conn.execute(
            """
            INSERT INTO orders (reference, data, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(reference) DO UPDATE SET data = excluded.data
            """,
            (reference, json.dumps(order), order.get("created_at", time.time())),
        )


def order_get(reference):
    """Return the stored order dict for this reference, or None."""
    with _db_conn() as conn:
        row = conn.execute(
            "SELECT data FROM orders WHERE reference = ?", (reference,)
        ).fetchone()
    return json.loads(row[0]) if row else None


def order_update(reference, **changes):
    """Merge `changes` into the existing order and persist it.
    Returns the updated order, or None if the reference doesn't exist."""
    order = order_get(reference)
    if order is None:
        return None
    order.update(changes)
    order_set(reference, order)
    return order


_init_orders_db()

# Substrings that, when they appear in a failed provider response, indicate
# the failure was OUR fault (e.g. low wallet balance with n3tdata) rather
# than something the customer did wrong. These get masked to a generic
# "contact admin" message so customers aren't told internal business details.
ADMIN_FAULT_MARKERS = [
    "insufficient balance",
    "insufficient wallet",
    "insufficient fund",
]


def _customer_message_for_failure(provider_message: str) -> str:
    """Decide what the customer sees for a failed provider fulfilment."""
    lowered = (provider_message or "").lower()
    if any(marker in lowered for marker in ADMIN_FAULT_MARKERS):
        return "Admin Error. Please contact admin."
    return ("We couldn't complete this order automatically. Please contact "
            "support with your reference number and we'll sort it out.")


def _initiate_paystack_payment(amount_naira, metadata):
    """Create a Paystack transaction with our own reference + callback_url.

    Returns (response_data, reference) on success. Raises PaystackError on
    failure so callers can turn it into a clean 400 response.
    """
    reference = f"VW_{uuid.uuid4().hex[:16]}"
    callback_url = request.url_root.rstrip("/") + "/order-status"

    header = {
        "Authorization": f"Bearer {PAYMENT_TOKEN}",
        "Content-Type": "Application/json"
    }
    payload = {
        "email": email,
        "amount": int(amount_naira) * 100,
        "reference": reference,
        "callback_url": callback_url,
        "metadata": metadata,
    }

    response = requests.post(url=PAYMENT_URL, json=payload, headers=header)
    response_data = response.json()
    print(f"{response_data}")

    if not response_data.get("status"):
        raise PaystackError(response_data.get("message", "Payment initialization failed"))

    return response_data, reference


class PaystackError(Exception):
    pass
@app.route("/airtime", methods=["GET"])
def airtime_page():
    # Looks directly inside your 'templates/' folder
    return render_template("airtime.html")


@app.route("/bots", methods=["GET"])
def bots():
    return render_template("bots.html")


@app.route("/website", methods=["GET"])
def website():
    return render_template("website.html")


@app.route("/dashboard", methods=["GET"])
def dashboard():
    return render_template("dashboard.html")

@app.route("/purchase_data", methods=["POST"])
def purchase_data():
    data = request.get_json()

    network = data.get("network")
    price = data.get("price")
    data_plan = data.get("data_plan")
    plan_id = data.get("id")
    plan_type = data.get("plan_type")
    phone = data.get("phone")
    duration = data.get("duration")

    if not all([network, price, data_plan, plan_id, phone]):
        return {"status": False, "message": "Missing required fields"}, 400

    # Guard against stale/placeholder frontend values (e.g. "test_id") reaching
    # Paystack — n3tdata requires plan_id to be a real numeric plan ID, and we'd
    # rather fail here than charge the customer for a plan we can't fulfil.
    if not str(plan_id).isdigit():
        return {"status": False, "message": "Invalid data plan selected. Please refresh the page and try again."}, 400

    metadata = {
        "type": "data",
        "network": network,
        "plan": data_plan,
        "plan_type": plan_type,
        "id": plan_id,
        "phone": phone,
        "duration": duration
    }

    try:
        response_data, reference = _initiate_paystack_payment(price, metadata)
    except PaystackError as e:
        return {"status": False, "message": str(e)}, 400

    order_set(reference, {
        "status": "pending",
        "network": network,
        "phone": phone,
        "plan_name": data_plan,
        "duration": duration,
        "price": price,
        "reference": reference,
        "customer_message": "We're confirming your payment and processing your order…",
        "admin_message": None,
        "created_at": time.time(),
    })
    return {
        "status": True,
        "message": "Payment link generated",
        "authorization_url": response_data["data"]["authorization_url"],
        "reference": response_data["data"]["reference"]
    }, 200


@app.route("/purchase_airtime", methods=["POST"])
def purchase_airtime():
    data = request.get_json()

    network = data.get("network")
    phone = data.get("phone")
    amount = data.get("amount")  # airtime value the customer wants delivered

    if not all([network, phone, amount]):
        return {"status": False, "message": "Missing required fields"}, 400

    try:
        airtime_amount = int(amount)
    except (TypeError, ValueError):
        return {"status": False, "message": "Invalid amount"}, 400

    if airtime_amount < AIRTIME_MIN_AMOUNT or airtime_amount > AIRTIME_MAX_AMOUNT:
        return {
            "status": False,
            "message": f"Amount must be between ₦{AIRTIME_MIN_AMOUNT} and ₦{AIRTIME_MAX_AMOUNT:,}"
        }, 400

    # This is the real charge — computed here, never taken from the client.
    pay_amount = _apply_airtime_discount(airtime_amount)

    # bypass is never taken from the client — always False, decided server-side
    metadata = {
        "type": "airtime",
        "network": network,
        "phone": phone,
        "amount": airtime_amount,   # full airtime value delivered to the customer
        "pay_amount": pay_amount    # what they were actually charged (after discount)
    }

    try:
        response_data, reference = _initiate_paystack_payment(pay_amount, metadata)
    except PaystackError as e:
        return {"status": False, "message": str(e)}, 400

    order_set(reference, {
        "status": "pending",
        "network": network,
        "phone": phone,
        "plan_name": f"{network} Airtime Top-up (₦{airtime_amount:,})",
        "duration": None,
        "price": pay_amount,
        "airtime_amount": airtime_amount,
        "discount_percent": AIRTIME_DISCOUNT_PERCENT,
        "reference": reference,
        "customer_message": "We're confirming your payment and processing your order…",
        "admin_message": None,
        "created_at": time.time(),
    })
    return {
        "status": True,
        "message": "Payment link generated",
        "authorization_url": response_data["data"]["authorization_url"],
        "reference": response_data["data"]["reference"]
    }, 200


@app.route("/payment-webhooks", methods=["POST"])
def payment_webhook():
    signature = request.headers.get('X-Paystack-Signature')
    if not signature:
        return {"message": "missing signature"}, 400

    raw_payload = request.get_data()

    expected = hmac.new(
        PAYMENT_TOKEN.encode('utf-8'),
        raw_payload,
        hashlib.sha512
    ).hexdigest()

    if not hmac.compare_digest(signature, expected):
        print("❌ Invalid signature")
        return {"message": "invalid signature"}, 200

    try:
        notification = request.get_json()
        event = notification.get("event")
        data = notification.get("data", {})

        if event == "charge.success":
            metadata = data.get("metadata", {})
            reference = data.get("reference")
            order = order_get(reference)

            purchase_type = metadata.get("type", "data")
            phone = metadata.get("phone")
            network_name = metadata.get("network")

            # Network ID mapping (update as needed)
            network_id_map = {
                "MTN": 1,
                "AIRTEL": 2,
                "GLO": 3,
                "9MOBILE": 4,
                "ETISALAT": 4
            }

            # Phone: Convert to international format (234XXXXXXXXXX) back to local
            clean_phone = (phone or "").strip()
            if clean_phone.startswith("234"):
                clean_phone = "0" + clean_phone[3:]
            if len(clean_phone) != 11 or not clean_phone.startswith("0"):
                clean_phone = phone  # fallback

            header = {
                "Authorization": PROVIDER_TOKEN,
                "Content-Type": "application/json"
            }

            if purchase_type == "airtime":
                # IMPORTANT: metadata["amount"] is the FULL airtime value the
                # customer is meant to receive — Paystack was only charged
                # metadata["pay_amount"] (the discounted price). The provider
                # always gets the full, undiscounted amount.
                amount = metadata.get("amount")
                pay_amount = metadata.get("pay_amount", amount)
                print(f"✅ Payment Success (airtime) → {network_name} | {phone} | "
                      f"delivering ₦{amount} (charged ₦{pay_amount})")

                payload = {
                    "network": network_id_map.get((network_name or "").upper(), 1),
                    "phone": clean_phone,
                    "plan_type": "VTU",
                    "amount": amount,
                    "bypass": False,
                    "request-id": reference or f"Airtime_{int(time.time())}"
                }
                print("📤 Sending to Provider (airtime):", payload)

                try:
                    response = requests.post(PROVIDER_URL_AIRTIME, json=payload, headers=header, timeout=15)
                    provider_response = response.json()
                except Exception as provider_err:
                    print(f"❌ Airtime provider request failed: {provider_err}")
                    provider_response = {"status": "fail", "message": str(provider_err)}

                print("📥 Provider Response (airtime):", provider_response)

            else:
                data_plan_name = metadata.get("plan") or metadata.get("data_plan")
                plan_id = metadata.get("id")

                print(f"✅ Payment Success (data) → {network_name} | {phone} | {data_plan_name} | plan_id={plan_id}")

                if not plan_id or not str(plan_id).isdigit():
                    print(f"❌ Invalid or missing plan_id in metadata: {plan_id!r} — cannot fulfil with provider")
                    if order:
                        order["status"] = "failed"
                        order["admin_message"] = f"Invalid plan_id in metadata: {plan_id!r}"
                        order["customer_message"] = _customer_message_for_failure("")
                        order_set(reference, order)
                    return {"message": "success"}, 200

                # n3tdata expects the provider's own plan_id (integer), not the plan name
                payload = {
                    "network": network_id_map.get((network_name or "").upper(), 1),
                    "phone": clean_phone,
                    "data_plan": int(plan_id),
                    "bypass": False,
                    "request-id": reference or f"Data_{int(time.time())}"
                }
                print("📤 Sending to Provider (data):", payload)

                try:
                    response = requests.post(PROVIDER_URL, json=payload, headers=header, timeout=15)
                    provider_response = response.json()
                except Exception as provider_err:
                    print(f"❌ Data provider request failed: {provider_err}")
                    provider_response = {"status": "fail", "message": str(provider_err)}

                print("📥 Provider Response (data):", provider_response)

            provider_status = str(provider_response.get("status", "")).lower()
            provider_message = provider_response.get("message", "")

            if order is None:
                # We lost track of this order (e.g. server restarted between
                # initiating payment and the webhook firing). Still worth
                # logging clearly since the customer was charged.
                print(f"⚠️ No local order record for reference {reference} — "
                      f"customer was charged but order-status page can't show details.")
            elif provider_status == "success":
                order["status"] = "success"
                order["admin_message"] = None
                order["customer_message"] = (
                    f"Your {order.get('plan_name', 'purchase')} was successful "
                    f"and has been delivered to {clean_phone}."
                )
                order_set(reference, order)
            else:
                order["status"] = "failed"
                order["admin_message"] = provider_message  # internal only — never sent to the client
                order["customer_message"] = _customer_message_for_failure(provider_message)
                order_set(reference, order)

            return {"message": "success"}, 200

        else:
            print(f"Other event: {event}")
            return {"message": "ignored"}, 200

    except Exception as e:
        print(f"Webhook error: {e}")
        return {"message": "error"}, 200


@app.route("/order-status")
def order_status_page():
    # Paystack appends ?reference=xxx&trxref=xxx to the callback_url
    reference = request.args.get("reference") or request.args.get("trxref") or ""
    return render_template("ticket.html", reference=reference)


@app.route("/api/order-status/<reference>")
def order_status_api(reference):
    order = order_get(reference)
    if not order:
        return jsonify({"status": "pending", "customer_message": "We're confirming your payment…"})

    # Never leak admin_message (the real provider failure reason) to the client
    safe = {k: v for k, v in order.items() if k != "admin_message"}
    return jsonify(safe)


@app.route("/")
def data_page():
    with open("plans.json", "r") as file:
        plans = json.load(file)

    # Group plans by network
    grouped = {
        "MTN": [],
        "AIRTEL": [],
        "GLO": [],
        "9MOBILE": []
    }

    for plan in plans:
        network = plan.get("network")
        if network in grouped:
            grouped[network].append({
                "plan": plan.get("plan_name"),
                "plan_id": plan.get("plan_id"),        # ← Important
                "plan_type": plan.get("plan_type"),
                "duration": plan.get("duration"),
                "price": plan.get("selling_price")
            })

    return render_template("data.html", grouped_plans=grouped)

if __name__ == "__main__":
    # Local dev only. On Render, the start command should be:
    #   gunicorn app:app
    # which ignores this block entirely and never runs with debug=True.
    debug_mode = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=debug_mode)
