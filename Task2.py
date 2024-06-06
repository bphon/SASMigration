import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os

# Define file paths
registry_path = r"G:\OneDrive\Desktop\AHS Work\SASMigration\CVX08AUG2023.csv"
person_path = r"G:\OneDrive\Desktop\AHS Work\SASMigration\CVX08AUG2023.csv"
output_path = r"G:\OneDrive\Desktop\AHS Work\SASMigration\project2output"
log_path = f"{output_path}\\Cervix_{datetime.now().strftime('%Y%m%d')}.log"

# Check if files exist
if not os.path.exists(registry_path):
    raise FileNotFoundError(f"Registry file not found: {registry_path}")
if not os.path.exists(person_path):
    raise FileNotFoundError(f"Person file not found: {person_path}")

# Load datasets
registry_df = pd.read_csv(registry_path)
person_df = pd.read_csv(person_path)

# Print column names to verify
print("Registry DataFrame Columns:", registry_df.columns)
print("Person DataFrame Columns:", person_df.columns)

# Filter registry data (Remove behaviour filter since the column is not available)
d1 = registry_df[(registry_df['icdo_top'].str.startswith('c53')) & 
                 (registry_df['reg_stat'] != 'review')]

# Merge with person data
d1 = d1.merge(person_df, on='acb_no', how='inner')
d1 = d1[(d1['sex'] == 'f') & (d1['age_diag'] >= 18)]

# Filter by diagnosis date
d1 = d1[(d1['diag_dte'] >= '1980-01-01') & (d1['diag_dte'] <= datetime.now().strftime('%Y-%m-%d'))]

# Save filtered data
cvx = f"cvx{datetime.now().strftime('%Y%m%d')}"
output_csv = f"{output_path}\\{cvx}.csv"
d1.to_csv(output_csv, index=False, columns=[
    'acb_no', 'reg_stat', 'mal_no', 'phn', 'DoB', 'DoB_stat', 'diag_dte', 
    'icdo_top', 'icdo_mor', 'icdo_grd', 'RHA_diag', 'vit_stat', 'dth_dte', 'dth_stat'
])

# Check if a new form needs to be filled out
chkdte = "REGISTRY REQUEST NEEDS TO BE FILLED OUT!" if datetime.now() > datetime(2016, 3, 1) else " "

# Create documentation
doc_content = f"""
{chkdte}
****************************************

Cervical Cancer Cases
Registry Request 2014-009, monthly runs

Results
 There are {len(d1)} cervical cancers representing {d1['acb_no'].nunique()} individuals
 on run date {datetime.now().strftime('%Y-%m-%d')}.

Criteria
  Site c53, any morphology
  Registry Status not in Review
  Female
  Alive or Dead
  Age at diagnosis >= 18
  Any Residence
  Cases diagnosed between 01JAN1980 and (approximately) {datetime.now().strftime('%Y-%m-%d')}
"""

documentation_path = f"{output_path}\\{cvx}.documentation.txt"
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
    from_email = "Tuyet.Thieu@albertahealthservices.ca"
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

    with smtplib.SMTP('localhost') as server:
        server.sendmail(from_email, to_emails, msg.as_string())

if myerror == 0:
    # Create copy of documentation file for requester
    destination_path = rf"/data/u01/sas/sasdata/Screening/S_R_dataUpdates/Cervix Update/{cvx}.documentation.txt"
    with open(documentation_path, 'r') as src, open(destination_path, 'w') as dst:
        dst.write(src.read())

    # Save copy of data to SASCommon directory for requester to access
    data_update_path = rf"/data/u01/sas/sasdata/Screening/S_R_dataUpdates/Cervix Update/{cvx}.csv"
    d1.to_csv(data_update_path, index=False)

    # Send log file email
    send_email(
        subject=f"Cervix Log from {datetime.now().strftime('%Y-%m-%d')}",
        body=f"This is the cervix run log file for {datetime.now().strftime('%Y-%m-%d')}.",
        to_emails=["angela.eckstrand@albertahealthservices.ca", "tuyet.thieu@albertahealthservices.ca"],
        attachments=[log_path]
    )

    # Send results email
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
            "linan.xu@albertahealthservices.ca",
            "ACB.cancerdata@albertahealthservices.ca",
            "angela.eckstrand@albertahealthservices.ca",
            "tuyet.thieu@albertahealthservices.ca",
            "Colby.Regel@albertahealthservices.ca",
            "Dan.Li@albertahealthservices.ca",
            "Yue.Wang@albertahealthservices.ca",
            "ChenChen.Sun@albertahealthservices.ca"
        ]
    )
else:
    # Send error email
    send_email(
        subject=f"ERRORS in Cervix run {datetime.now().strftime('%Y-%m-%d')} 2014-009",
        body=f"The cervix run log of {datetime.now().strftime('%Y-%m-%d')} contained errors. No data was sent. The log file is attached.",
        to_emails=["angela.eckstrand@albertahealthservices.ca", "tuyet.thieu@albertahealthservices.ca"],
        attachments=[log_path]
    )
