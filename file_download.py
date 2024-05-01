import requests
import json
from msal import ConfidentialClientApplication
from pathlib import Path
from datetime import datetime, timedelta
import pytz
import schedule
import time
import base64
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set your app credentials
CLIENT_ID = 'YOUR_CLIENT_ID'
CLIENT_SECRET = 'YOUR_CLIENT_SECRET'
TENANT_ID = 'YOUR_TENANT_ID'
AUTHORITY = f'https://login.microsoftonline.com/{TENANT_ID}'
SCOPE = ['https://graph.microsoft.com/.default']
USER_EMAIL = 'YOUR_USER_EMAIL'

# Set up paths for files
DOWNLOAD_FOLDER_1 = Path('')
DOWNLOAD_FOLDER_2 = Path('')
DOWNLOADED_ATTACHMENTS_FILE = Path('downloaded_attachments.txt')
LOG_FILE = Path('download_log.txt')
TIMESTAMP_FILE = Path('last_run_timestamp.txt')

# Initialize set for processed emails
processed_emails = set()

def acquire_token():
    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET,
    )
    result = app.acquire_token_for_client(scopes=SCOPE)

    if 'access_token' in result:
        return result['access_token']
    else:
        error_description = result.get('error_description', 'No error description provided')
        raise Exception(f"Error acquiring access token: {error_description}")

def download_attachments():
    global processed_emails

    logger.info("Processed Emails: %s", processed_emails)

    # Read the last run timestamp from the file
    default_timestamp = "2023-01-01 00:00:00"
    try:
        with open(TIMESTAMP_FILE, 'r') as file:
            timestamp_str = file.read().strip()
            if timestamp_str:
                last_run_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                last_run_timestamp = pytz.utc.localize(last_run_timestamp).astimezone(pytz.timezone('Europe/Berlin'))
            else:
                logger.warning("Timestamp not found in the file. Using default timestamp.")
                last_run_timestamp = datetime.strptime(default_timestamp, '%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        logger.error("Error parsing timestamp: %s", e)
        last_run_timestamp = datetime.strptime(default_timestamp, '%Y-%m-%d %H:%M:%S')

    # Calculate the timestamp for 72 hours ago
    start_time = datetime.now(pytz.utc) - timedelta(hours=72)
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    try:
        access_token = acquire_token()
        headers = {'Authorization': 'Bearer ' + access_token, 'Content-Type': 'application/json'}

        downloaded_attachments = set()
        if DOWNLOADED_ATTACHMENTS_FILE.is_file():
            with open(DOWNLOADED_ATTACHMENTS_FILE, 'r', encoding='utf-8') as file:
                downloaded_attachments = set(file.read().splitlines())

        logged_attachments = set()
        if LOG_FILE.is_file():
            with open(LOG_FILE, 'r', encoding='utf-8') as file:
                for line in file:
                    parts = line.strip().split(', ')
                    if len(parts) >= 2:
                        logged_attachments.add(parts[1].split(': ')[1])

        messages_url = f'https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/messages?$select=id,createdDateTime&$filter=receivedDateTime ge {start_time_str} and hasAttachments eq true'
        while messages_url:
            try:
                response = requests.get(messages_url, headers=headers, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error("Error fetching messages: %s", e)
                break

            if response.status_code == 200:
                messages_data = json.loads(response.text)
                messages = messages_data.get('value', [])
                for message in messages:
                    message_id = message.get('id')
                    sent_time_str = message.get('createdDateTime')
                    if message_id and sent_time_str:
                        sent_time = datetime.fromisoformat(sent_time_str[:-1])
                        sent_time = pytz.utc.localize(sent_time).astimezone(pytz.timezone('Europe/Berlin'))

                        logger.info("Processing message with ID: %s, Sent time: %s", message_id, sent_time)

                        if message_id in processed_emails:
                            logger.info("Skipping already processed email with ID: %s", message_id)
                            continue

                        if message_id not in downloaded_attachments:
                            try:
                                attachments_response = requests.get(f'https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/messages/{message_id}/attachments', headers=headers, timeout=10)
                                attachments_response.raise_for_status()
                            except requests.exceptions.RequestException as e:
                                logger.error("Error fetching attachments for message '%s': %s", message_id, e)
                                continue

                            if attachments_response.status_code == 200:
                                attachments_data = json.loads(attachments_response.text)
                                attachments = attachments_data.get('value', [])
                                for attachment in attachments:
                                    if attachment.get('contentBytes') and attachment.get('name', '').lower().endswith('.ddd'):
                                        attachment_filename = attachment.get('name', '')
                                        attachment_filename_encoded = attachment_filename.encode('utf-8', 'replace').decode('utf-8')

                                        if attachment_filename not in downloaded_attachments and attachment_filename_encoded not in logged_attachments:
                                            with open(DOWNLOAD_FOLDER_1 / attachment_filename_encoded, 'wb') as file:
                                                file.write(base64.b64decode(attachment['contentBytes']))
                                            logger.info("Attachment '%s' saved to %s", attachment_filename_encoded, DOWNLOAD_FOLDER_1)

                                            formatted_sent_time = sent_time.strftime('%Y-%m-%d %H:%M:%S')
                                            with open(LOG_FILE, 'a', encoding='utf-8') as log:
                                                log.write(f"Downloaded file: {attachment_filename_encoded}, Message ID: {message_id}, Sent time: {formatted_sent_time}\n")

                                            downloaded_attachments.add(attachment_filename)
                                            processed_emails.add(message_id)
                                        else:
                                            logger.info("Skipping duplicate attachment: '%s'", attachment_filename_encoded)
                                    else:
                                        logger.info("Skipping non-.ddd attachment: '%s'", attachment.get('name', ''))
                            else:
                                logger.error("Error fetching attachments for message '%s': %s - %s", message_id, attachments_response.status_code, attachments_response.text)
                        else:
                            logger.info("Message ID already processed.")
                    else:
                        logger.error("Message ID or Sent time not found in the response.")

                messages_url = messages_data.get('@odata.nextLink')
            else:
                logger.error("Error fetching messages: %s - %s", response.status_code, response.text)

        with open(DOWNLOADED_ATTACHMENTS_FILE, 'w', encoding='utf-8') as file:
            file.write('\n'.join(downloaded_attachments))

        current_timestamp = datetime.now(pytz.timezone('Europe/Berlin')).strftime('%Y-%m-%d %H:%M:%S')
        with open(TIMESTAMP_FILE, 'w', encoding='utf-8') as file:
            file.write(current_timestamp)
    except Exception as e:
        logger.error("An error occurred: %s", e)

# Schedule the job to run every 1 minute
schedule.every(30).minutes.do(download_attachments)

# Run the scheduler continuously with a countdown message every 60 seconds
try:
    while True:
        countdown = 1800
        while countdown > 0:
            if countdown % 60 == 0:
                logger.info("Next check in %s seconds...", countdown)
            time.sleep(1)
            countdown -= 1
        schedule.run_pending()
except KeyboardInterrupt:
    logger.info("\nScript interrupted. Exiting...")
