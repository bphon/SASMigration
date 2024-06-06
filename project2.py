import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
import logging
from datetime import datetime

# Define file paths
output_path = r"G:\OneDrive\Desktop\AHS Work\SASMigration\project2output"
log_path = os.path.join(output_path, f"Cervix_{datetime.now().strftime('%Y%m%d')}.log")
documentation_path = os.path.join(output_path, f"cvx{datetime.now().strftime('%Y%m%d')}.documentation.txt")

# Ensure the output directory exists
os.makedirs(output_path, exist_ok=True)

# Create a log file
logging.basicConfig(filename=log_path, level=logging.INFO)
logging.info("This is a log file for the Cervix project")

# Generate documentation content
doc_content = f"""
****************************************

Cervical Cancer Cases
Registry Request 2014-009, monthly runs

Results
 There are X cervical cancers representing Y individuals
 on run date {datetime.now().strftime('%Y-%m-%d')}.

Criteria
  Site c53, any morphology
  Behaviour /1 /2 /3
  Registry Status not in Review
  Female
  Alive or Dead
  Age at diagnosis >= 18
  Any Residence
  Cases diagnosed between 01JAN1980 and (approximately) {datetime.now().strftime('%Y-%m-%d')}
"""

with open(documentation_path, 'w') as f:
    f.write(doc_content)

# Check for errors in log file
myerror = 0
if os.path.exists(log_path):
    with open(log_path, 'r') as log_file:
        for line in log_file:
            if 'error' in line.lower():
                myerror = 1
                break

# Send emails based on error status
def send_email(subject, body, to_emails, attachments=[]):
    from_email = "your_email@example.com"
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = ", ".join(to_emails)
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    for attachment in attachments:
        with open(attachment, 'rb') as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(attachment))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment)}"'
            msg.attach(part)

    with smtplib.SMTP('localhost', 1025) as server:
        server.sendmail(from_email, to_emails, msg.as_string())

if myerror == 0:
    # No errors: send results to clients
    send_email(
        subject=f"Cervix Results from {datetime.now().strftime('%Y-%m-%d')} 2014-009",
        body=f"""
        Dear All,

        Here is this month's cervix run. The dataset and documentation
        are in the /data/u01/sas/sasdata/Screening/S_R_dataUpdates/Cervix Update directory.
        Thank you,

        --
        """,
        to_emails=[
            "client1@example.com",
            "client2@example.com",
            "client3@example.com"
        ],
        attachments=[documentation_path]
    )
else:
    # Errors found: send error notification to analyst
    send_email(
        subject=f"ERRORS in Cervix run {datetime.now().strftime('%Y-%m-%d')} 2014-009",
        body=f"The cervix run log of {datetime.now().strftime('%Y-%m-%d')} contained errors. No data was sent. The log file is attached.",
        to_emails=["analyst@example.com"],
        attachments=[log_path]
    )

print("Email(s) sent based on the error status.")
