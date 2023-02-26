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


load_dotenv()



current_filepath    =   Path(__file__)#.stem
# current_filepath    =   Path(__file__).stem

CURRENT_TIMESTAMP   =   datetime.now().strftime("%Y-%m-%d %H:%M:%S")
EMAIL_ADDRESS       =   os.getenv("SENDER")
EMAIL_PASSWORD      =   os.getenv("EMAIL_PASSWORD")

message = MIMEMultipart()
message["From"] = "Postgres Data Warehouse Program - SDW"
message["To"] = EMAIL_ADDRESS
message["Subject"] = f"L0 - Travel Data Generation Log - {CURRENT_TIMESTAMP}"



# Add body to the email message
body_main_subject = "extracting the travel data from the source systems"
body = f"""Hi Stephen, 
See attached the logs for {body_main_subject}. 
"""
message.attach(MIMEText(body, "plain"))


print(f'File path: {current_filepath} ')
print(f'Sender: {message["From"]}')
print(f'Recipient: {message["To"]} ')
print(f'Subject: {message["Subject"]}')


log_directory = os.getenv("L1_LOG_DIRECTORY")

for log_filename in os.listdir(log_directory):
    if os.path.isfile(os.path.join(log_directory, log_filename)):
        print(f'Log filepath: {os.path.join(log_directory, log_filename)} ')


# with smtplib.SMTP(host="smtp.gmail.com", port=587) as smtp:
#     smtp.ehlo()
#     smtp.starttls()
#     smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)