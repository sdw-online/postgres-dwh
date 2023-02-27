import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from email import encoders
from pathlib import Path
from dotenv import load_dotenv


# Load environment variables from .env 
load_dotenv()



# Set up constants 
current_filepath    =   Path(__file__).stem

SMTP_PORT           =   587
SMTP_HOST_SERVER    =   "smtp.gmail.com"
CURRENT_TIMESTAMP   =   datetime.now().strftime("%Y-%m-%d %H:%M:%S")
EMAIL_ADDRESS       =   os.getenv("SENDER")
EMAIL_PASSWORD      =   os.getenv("EMAIL_PASSWORD")
SENDER              =   "Postgres Data Warehouse Program - SDW"
RECIPIENT           =   os.getenv("RECIPIENT")


L0_LOG_DIRECTORY    =   os.getenv("L0_LOG_DIRECTORY")
L1_LOG_DIRECTORY    =   os.getenv("L1_LOG_DIRECTORY")
L2_LOG_DIRECTORY    =   os.getenv("L2_LOG_DIRECTORY")
L3_LOG_DIRECTORY    =   os.getenv("L3_LOG_DIRECTORY")
L4_LOG_DIRECTORY    =   os.getenv("L4_LOG_DIRECTORY")

body_main_subject   =   "loading data from raw tables into the staging tables of the Postgres data warehouse"
body                =   f"""Hi Stephen, 

See attached the logs for {body_main_subject}. 

Regards,
{SENDER}

"""


# Create function for getting the directory paths for log files
def get_log_filepaths(log_directory):
    log_filepaths = []
    for root, directories, log_files in os.walk(log_directory):
        for filename in log_files:
            log_filepath = os.path.join(root, filename)
            log_filepaths.append(log_filepath)
    return log_filepaths


# Create function for attaching log files to email 
def attach_log_files_to_email(message, log_filepaths):
    for log_file in log_filepaths:
        with open(log_file, 'rb') as file:
            log_attachment = MIMEBase('application', 'octet-stream')
            log_attachment.set_payload(file.read())
            encoders.encode_base64(log_attachment)
            log_attachment.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(log_file)}"')
            message.attach(log_attachment)
  

  

# ===================================== SETTING UP LOG FILE ATTACHMENTS ===================================== 

   
# Get directory paths for log files
staging_layer_log_directory = get_log_filepaths(L2_LOG_DIRECTORY)

log_file_counter = 0
for log_file in staging_layer_log_directory:
    log_file_counter += 1
    print('')
    print(f'Log file {log_file_counter}: {log_file} ')



# ===================================== SETTING UP EMAIL MESSAGE ===================================== 

# Set up constants for email 
message = MIMEMultipart()
message["From"]         =   SENDER
message["To"]           =   RECIPIENT
message["Subject"]      =   f"L2 - Staging Layer Log Files - {CURRENT_TIMESTAMP}"


# Add body to the email message
message.attach(MIMEText(body, "plain"))


# Attach log files to email
attach_log_files_to_email(message, staging_layer_log_directory)



# ===================================== SENDING EMAIL MESSAGE ===================================== 

def send_email():
    with smtplib.SMTP(host=SMTP_HOST_SERVER, port=SMTP_PORT) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(message)
        print('Message sent successfully. ')
        print()


send_email()


