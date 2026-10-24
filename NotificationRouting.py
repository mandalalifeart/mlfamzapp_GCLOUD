import os
import smtplib
from email.mime.text import MIMEText

import requests

POCKETBASE_URL = os.environ.get("POCKETBASE_URL", "").rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ.get("POCKETBASE_ADMIN_EMAIL", "")
POCKETBASE_ADMIN_PASSWORD = os.environ.get("POCKETBASE_ADMIN_PASSWORD", "")

GMAIL_USER = os.environ.get("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
REPORT_EMAIL_TO = os.environ.get("REPORT_EMAIL_TO", "")
TELEGRAM_BOT_TOKEN = (os.environ.get("UPDATE_TELEGRAM_BOT_TOKEN") or os.environ.get("MCF_TELEGRAM_BOT_TOKEN", ""))
TELEGRAM_CHAT_ID = (os.environ.get("UPDATE_TELEGRAM_CHAT_ID") or os.environ.get("MCF_TELEGRAM_CHAT_ID", ""))

# Fallback policy when a job has no scheduled_jobs config row yet -
# matches the standing "Telegram gets everything, email is alerts/errors
# only" preference this whole project already follows by default.
# @baba_social_bot (per the user 2026-10-21: the daily sales digest and the
# daily Ads digest go there, everything else stays on @baba_update_bot).
SOCIAL_TELEGRAM_BOT_TOKEN = os.environ.get("MCF_TELEGRAM_BOT_TOKEN", "")
SOCIAL_TELEGRAM_CHAT_ID = os.environ.get("MCF_TELEGRAM_CHAT_ID", "")
# @baba_amz_bot (2026-10-22, per the user): all Amazon sales updates (hourly
# sales, daily sales digest, daily Ads digest). Falls back to the social bot
# if the new bot can't deliver (e.g. the chat wasn't started yet).
AMZ_TELEGRAM_BOT_TOKEN = os.environ.get("AMZ_TELEGRAM_BOT_TOKEN", "")
AMZ_TELEGRAM_CHAT_ID = os.environ.get("AMZ_TELEGRAM_CHAT_ID", "")


def send_amz_telegram(method, data=None, json_body=None, files=None, timeout=30):
    """Send via @baba_amz_bot, falling back to @baba_social_bot on failure."""
    pairs = [(AMZ_TELEGRAM_BOT_TOKEN, AMZ_TELEGRAM_CHAT_ID), (SOCIAL_TELEGRAM_BOT_TOKEN, SOCIAL_TELEGRAM_CHAT_ID)]
    resp = None
    for token, chat_id in pairs:
        if not token or not chat_id:
            continue
        if files:
            for f in files.values():
                f[1].seek(0)
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/{method}",
            data={**(data or {}), "chat_id": chat_id} if data is not None or files else None,
            json={**json_body, "chat_id": chat_id} if json_body is not None else None,
            files=files, timeout=timeout,
        )
        if resp.ok:
            return resp
    return resp

DEFAULT_CONFIG = {"notify_telegram": True, "notify_email_on_error": True, "notify_email_always": False}


def pb_authenticate():
    resp = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def get_job_config(job_name, token=None):
    token = token or pb_authenticate()
    resp = requests.get(
        f"{POCKETBASE_URL}/api/collections/scheduled_jobs/records",
        headers={"Authorization": token},
        params={"filter": f'job_name = "{job_name}"', "perPage": 1},
        timeout=15,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])
    if not items:
        return dict(DEFAULT_CONFIG)
    row = items[0]
    return {
        "notify_telegram": row.get("notify_telegram", DEFAULT_CONFIG["notify_telegram"]),
        "notify_email_on_error": row.get("notify_email_on_error", DEFAULT_CONFIG["notify_email_on_error"]),
        "notify_email_always": row.get("notify_email_always", DEFAULT_CONFIG["notify_email_always"]),
    }


def _send_telegram(text, bot="update", parse_mode=None):
    if bot == "amazon":
        body = {"text": text[:4000]}
        if parse_mode:
            body["parse_mode"] = parse_mode
        resp = send_amz_telegram("sendMessage", json_body=body, timeout=15)
        if parse_mode and resp is not None and not resp.ok:  # bad markup -> send as plain text
            send_amz_telegram("sendMessage", json_body={"text": text[:4000]}, timeout=15)
        return
    token, chat_id = (
        (SOCIAL_TELEGRAM_BOT_TOKEN, SOCIAL_TELEGRAM_CHAT_ID) if bot == "social"
        else (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    )
    if not token or not chat_id:
        return
    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": text[:4000]},
        timeout=15,
    )


def _send_email(subject, text):
    if not (GMAIL_USER and GMAIL_APP_PASSWORD and REPORT_EMAIL_TO):
        return
    msg = MIMEText(text)
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"] = REPORT_EMAIL_TO
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, [REPORT_EMAIL_TO], msg.as_string())


def _log_run(token, job_name, app, status, summary, notified_telegram, notified_email):
    try:
        import datetime
        requests.post(
            f"{POCKETBASE_URL}/api/collections/job_runs/records",
            headers={"Authorization": token},
            json={
                "job_name": job_name,
                "app": app,
                "ran_at": datetime.datetime.utcnow().isoformat() + "Z",
                "status": status,
                "summary": (summary or "")[:2000],
                "notified_telegram": notified_telegram,
                "notified_email": notified_email,
            },
            timeout=15,
        )
    except Exception:
        pass  # logging the run must never break the caller's real work


def notify(job_name, app, text, is_error=False, subject=None, status=None, telegram=True, bot="update", parse_mode=None):
    """Send text through whichever channels this job is configured for
    (falling back to the standing telegram-always/email-on-error-only
    policy if the job has no scheduled_jobs row yet), and record one
    job_runs entry either way. Returns nothing - notification/logging
    failures are swallowed so they never break the caller's real work.
    telegram=False lets a caller skip Telegram for an uneventful run even
    when the job has Telegram enabled (e.g. Etsy jobs only message when
    something actually happened - per the user 2026-10-02)."""
    status = status or ("error" if is_error else "success")
    try:
        token = pb_authenticate()
        config = get_job_config(job_name, token)
    except Exception:
        token = None
        config = dict(DEFAULT_CONFIG)

    sent_telegram = False
    sent_email = False

    if config["notify_telegram"] and telegram:
        try:
            _send_telegram(text, bot, parse_mode)
            sent_telegram = True
        except Exception:
            pass

    if config["notify_email_always"] or (is_error and config["notify_email_on_error"]):
        try:
            _send_email(subject or f"{job_name} - {status}", text)
            sent_email = True
        except Exception:
            pass

    if token:
        _log_run(token, job_name, app, status, text, sent_telegram, sent_email)
