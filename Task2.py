# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 13:47:00 2024

@author: brianphong
"""

import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# Define file paths
registry_path = "/data/u01/sas/sasdata/CMORE/ACR/Dataset_Creation/Data/malig.csv"
person_path = "/data/u01/sas/sasdata/CMORE/ACR/Dataset_Creation/Data/person.csv"
output_path = "/data/u01/sas/sasdata/CMORE/ACRONLY/Projects/Cancer Screening/Cervix/Output/"
log_path = f"{output_path}Cervix_{datetime.now().strftime('%Y%m%d')}.log"

# Load datasets
registry_df = pd.read_csv(registry_path)
person_df = pd.read_csv(person_path)

# Filter registry data
d1 = registry_df[(registry_df['icdo_top'].str.startswith('c53')) & 
                 (registry_df['behaviour'].isin([1, 2, 3])) & 
                 (registry_df['reg_stat'] != 'review')]

# Merge with person data
d1 = d1.merge(person_df, on='acb_no', how='inner')
d1 = d1[(d1['sex'] == 'f') & (d1['age_diag'] >= 18)]

# Filter by diagnosis date
d1 = d1[(d1['diag_dte'] >= '1980-01-01') & (d1['diag_dte'] <= datetime.now().strftime('%Y-%m-%d'))]

# Save filtered data
cvx = f"cvx{datetime.now().strftime('%Y%m%d')}"
d1.to_csv(f"{output_path}{cvx}.csv", index=False, columns=[
    'acb_no', 'reg_stat', 'mal_no', 'phn', 'DoB', 'DoB_stat', 'diag_dte', 
    'icdo_top', 'icdo_mor', 'icdo_grd', 'rha_diag', 'vit_stat', 'dth_dte', 'dth_stat'
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
  Behaviour /1 /2 /3
  Registry Status not in Review
  Female
  Alive or Dead
  Age at diagnosis >= 18
  Any Residence
  Cases diagnosed between 01JAN1980 and (approximately) {datetime.now().strftime('%Y-%m-%d')}
"""

with open(f"{output_path}{cvx}.documentation.txt", 'w') as f:
    f.write(doc_content)

# Check for errors in log file
myerror = 0
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
            part = MIMEApplication(f.read(), Name=attachment)
            part['Content-Disposition'] = f'attachment; filename="{attachment.split("/")[-1]}"'
            msg.attach(part)

    with smtplib.SMTP('localhost') as server:
        server.sendmail(from_email, to_emails, msg.as_string())

if myerror == 0:
    # Create copy of documentation file for requester
    with open(f"{output_path}{cvx}.documentation.txt", 'r') as src, open(f"/data/u01/sas/sasdata/Screening/S_R_dataUpdates/Cervix Update/{cvx}.documentation.txt", 'w') as dst:
        dst.write(src.read())

    # Save copy of data to SASCommon directory for requester to access
    d1.to_csv(f"/data/u01/sas/sasdata/Screening/S_R_dataUpdates/Cervix Update/{cvx}.csv", index=False)

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





