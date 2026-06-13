"""
send_emails.py
Sends personalized cold emails via Resend API from a CSV contact list.

CSV format (contacts.csv):
first_name,email,company
John,john@company.com,Acme Inc

Usage:
    python send_emails.py              # sends to all contacts
    python send_emails.py --test       # sends only to yourself first
"""

import os
import csv
import time
import logging
import argparse
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / '.env')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────────
RESEND_API_KEY  = os.getenv('RESEND_API_KEY', 're_aTKzvCYU_FWgEPZak5AHgq9QAUdXrMCTC')
FROM_EMAIL      = os.getenv('FROM_EMAIL', 'Prajwol from InferReach <prajwol@inferreach.com>')
TEST_EMAIL      = os.getenv('TEST_EMAIL', 'prajwol@inferreach.com')
CONTACTS_FILE   = os.getenv('CONTACTS_FILE', 'contacts.csv')
DELAY_SECONDS   = 2   # delay between emails to avoid spam flags

# ── EMAIL TEMPLATE ────────────────────────────────────────
def get_subject():
    return "quick question about your data stack"

def get_body(first_name, company=None):
    name = first_name.strip().capitalize()
    return f"""Hi {name},

Broken pipelines, stale dashboards, data your team no longer trusts.

That is what most companies are quietly dealing with.

I run InferReach. We build and manage data infrastructure end to end so your team does not have to think about it.

Happy to do a free 30-minute audit of your current setup, no commitment, just honest recommendations you can act on.

Worth a quick chat?

Prajwol
inferreach.com
hello@inferreach.com"""

def get_html_body(first_name, company=None):
    name = first_name.strip().capitalize()
    return f"""
<div style="font-family: Arial, sans-serif; font-size: 15px; color: #1a1a1a; line-height: 1.7; max-width: 560px;">
  <p>Hi {name},</p>
  <p>Broken pipelines, stale dashboards, data your team no longer trusts.</p>
  <p>That is what most companies are quietly dealing with.</p>
  <p>I run <a href="https://inferreach.com" style="color:#10b981;">InferReach</a>. We build and manage data infrastructure end to end so your team does not have to think about it.</p>
  <p>Happy to do a free 30-minute audit of your current setup, no commitment, just honest recommendations you can act on.</p>
  <p>Worth a quick chat?</p>
  <p>
    Prajwol<br/>
    <a href="https://inferreach.com" style="color:#10b981;">inferreach.com</a><br/>
    hello@inferreach.com
  </p>
</div>
"""

# ── SEND ──────────────────────────────────────────────────
def send_email(to_email, first_name, company=None):
    payload = {
        'from':    FROM_EMAIL,
        'to':      [to_email],
        'subject': get_subject(),
        'text':    get_body(first_name, company),
        'html':    get_html_body(first_name, company),
    }
    try:
        r = requests.post(
            'https://api.resend.com/emails',
            headers={
                'Authorization': f'Bearer {RESEND_API_KEY}',
                'Content-Type':  'application/json',
            },
            json=payload,
            timeout=10
        )
        r.raise_for_status()
        log.info(f'Sent to {to_email} ({first_name}) — id: {r.json().get("id")}')
        return True
    except Exception as e:
        log.error(f'Failed to send to {to_email}: {e}')
        if hasattr(e, 'response') and e.response is not None:
            log.error(f'Response: {e.response.text}')
        return False

# ── LOAD CONTACTS ─────────────────────────────────────────
def load_contacts(filepath):
    contacts = []
    try:
        with open(filepath, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                email      = row.get('email', '').strip()
                first_name = row.get('first_name', '').strip() or 'there'
                company    = row.get('company', '').strip()
                if email and '@' in email:
                    contacts.append({'email': email, 'first_name': first_name, 'company': company})
    except FileNotFoundError:
        log.error(f'contacts.csv not found. Create it with columns: first_name, email, company')
    return contacts

# ── MAIN ──────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true', help='Send only to test email first')
    args = parser.parse_args()

    if args.test:
        log.info(f'TEST MODE — sending to {TEST_EMAIL} only')
        send_email(TEST_EMAIL, 'Prajwol', 'InferReach')
        return

    contacts = load_contacts(CONTACTS_FILE)
    if not contacts:
        log.error('No contacts found. Check your contacts.csv file.')
        return

    log.info(f'Loaded {len(contacts)} contacts from {CONTACTS_FILE}')
    log.info('Starting send... (2 second delay between emails)')

    sent = 0
    failed = 0

    for i, contact in enumerate(contacts, 1):
        log.info(f'[{i}/{len(contacts)}] Sending to {contact["email"]}...')
        ok = send_email(contact['email'], contact['first_name'], contact.get('company'))
        if ok:
            sent += 1
        else:
            failed += 1
        if i < len(contacts):
            time.sleep(DELAY_SECONDS)

    log.info(f'Done. Sent: {sent} | Failed: {failed}')

if __name__ == '__main__':
    main()