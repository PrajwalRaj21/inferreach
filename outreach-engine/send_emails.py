"""
sender.py — InferReach cold email sender
Reads Apollo.io CSV export and sends personalized emails via Resend.
"""

import os, csv, time, logging, argparse, requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / '.env')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

RESEND_API_KEY = os.getenv('RESEND_API_KEY', 're_aTKzvCYU_FWgEPZak5AHgq9QAUdXrMCTC')
FROM_EMAIL     = os.getenv('FROM_EMAIL', 'Prajwol from InferReach <prajwol@inferreach.com>')
TEST_EMAIL     = os.getenv('TEST_EMAIL', 'prajwol@inferreach.com')
CONTACTS_FILE  = os.getenv('CONTACTS_FILE', 'contacts.csv')
DELAY_SECONDS  = 2

def get_body(first_name):
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

def get_html(first_name):
    name = first_name.strip().capitalize()
    return f"""<div style="font-family:Arial,sans-serif;font-size:15px;color:#1a1a1a;line-height:1.7;max-width:560px">
<p>Hi {name},</p>
<p>Broken pipelines, stale dashboards, data your team no longer trusts.</p>
<p>That is what most companies are quietly dealing with.</p>
<p>I run <a href="https://inferreach.com" style="color:#10b981">InferReach</a>. We build and manage data infrastructure end to end so your team does not have to think about it.</p>
<p>Happy to do a free 30-minute audit of your current setup, no commitment, just honest recommendations you can act on.</p>
<p>Worth a quick chat?</p>
<p>Prajwol<br><a href="https://inferreach.com" style="color:#10b981">inferreach.com</a><br>hello@inferreach.com</p>
</div>"""

def send(to_email, first_name):
    try:
        r = requests.post('https://api.resend.com/emails',
            headers={'Authorization': f'Bearer {RESEND_API_KEY}', 'Content-Type': 'application/json'},
            json={'from': FROM_EMAIL, 'to': [to_email],
                  'subject': 'quick question about your data stack',
                  'text': get_body(first_name), 'html': get_html(first_name)},
            timeout=10)
        r.raise_for_status()
        log.info(f'Sent to {to_email} ({first_name}) — id: {r.json().get("id")}')
        return True
    except Exception as e:
        log.error(f'Failed {to_email}: {e}')
        return False

def load_contacts(filepath):
    contacts = []
    try:
        with open(filepath, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Support Apollo export format, simple format, and AI Ark format
                email  = (row.get('Email Business') or row.get('Email') or row.get('email') or '').strip()
                fname  = (row.get('First Name') or row.get('first_name') or 'there').strip()
                status = (row.get('Business Status') or row.get('Email Status') or row.get('email_status') or 'verified').strip().lower()

                # Skip unavailable emails
                if not email or '@' not in email:
                    continue
                if status == 'unavailable':
                    log.info(f'Skipping {email} — unavailable')
                    continue
                contacts.append({'email': email, 'first_name': fname})
    except FileNotFoundError:
        log.error(f'{filepath} not found.')
    return contacts

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true')
    args = parser.parse_args()

    if args.test:
        log.info(f'TEST MODE — sending to {TEST_EMAIL} only')
        send(TEST_EMAIL, 'Prajwol')
        return

    contacts = load_contacts(CONTACTS_FILE)
    if not contacts:
        log.error('No valid contacts found.')
        return

    log.info(f'{len(contacts)} contacts loaded. Sending...')
    sent = failed = 0
    for i, c in enumerate(contacts, 1):
        log.info(f'[{i}/{len(contacts)}] {c["email"]}')
        if send(c['email'], c['first_name']): sent += 1
        else: failed += 1
        if i < len(contacts): time.sleep(DELAY_SECONDS)

    log.info(f'Done. Sent: {sent} | Failed: {failed} | Skipped unavailable emails automatically')

if __name__ == '__main__':
    main()