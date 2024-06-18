import pandas as pd
from datetime import datetime
import os
import snowflake.connector

# Define file paths
registry_path = ""
person_path = ""
output_path = ""
log_path = os.path.join(output_path, f"Cervix_{datetime.now().strftime('%Y%m%d')}.log")

# Check if files exist
if not os.path.exists(registry_path):
    raise FileNotFoundError(f"Registry file not found: {registry_path}")
if not os.path.exists(person_path):
    raise FileNotFoundError(f"Person file not found: {person_path}")

# Create output directory if it does not exist
os.makedirs(output_path, exist_ok=True)

# Load datasets
registry_df = pd.read_csv(registry_path)
person_df = pd.read_csv(person_path)

# Print column names to verify
print("Registry DataFrame Columns:", registry_df.columns)
print("Person DataFrame Columns:", person_df.columns)

# Convert date columns to datetime objects
person_df['diag_dte'] = pd.to_datetime(person_df['diag_dte'], format='%d%b%Y')
person_df['dth_dte'] = pd.to_datetime(person_df['dth_dte'], format='%d%b%Y', errors='coerce')
person_df['DoB'] = pd.to_datetime(person_df['DoB'], format='%d%b%Y')

# Filter registry data (Remove behaviour filter since the column is not available)
d1 = registry_df[(registry_df['icdo_top'].str.startswith('c53')) & 
                 (registry_df['reg_stat'] != 'review')]


# Merge with person data
d1 = d1.merge(person_df, on='acb_no', how='inner')
print(d1.head())

# Print columns of the merged DataFrame to verify
print("Merged DataFrame Columns:", d1.columns)

# Ensure that columns 'sex' and 'age_diag' exist in the merged DataFrame
print("Merged DataFrame First Few Rows:", d1.head())

# Filtering the merged DataFrame
d1 = d1[(d1['sex'] == 'f') & (d1['age_diag'] >= 18)]

# Filter by diagnosis date
d1 = d1[(d1['diag_dte'] >= pd.to_datetime('01Jan1980', format='%d%b%Y')) & 
        (d1['diag_dte'] <= datetime.now())]

# Save filtered data
cvx = f"cvx{datetime.now().strftime('%Y%m%d')}"
output_csv = os.path.join(output_path, f"{cvx}.csv")
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

documentation_path = os.path.join(output_path, f"{cvx}.documentation.txt")
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

# Function to send emails using Snowflake
def send_email_via_snowflake(subject, body, to_emails):
    ctx = snowflake.connector.connect(
            account = '',
            user='',
            password='',
            role='',
            warehouse=''
    )
    cs = ctx.cursor()
    email_body = body.replace("\n", "\\n").replace("\r", "\\r")
    to_emails_str = ", ".join(to_emails)
    try:
        cs.execute(f"""
            call system$send_email(
                'my_email_int',
                '{to_emails_str}',
                '{subject}',
                '{email_body}'
            )
        """)
    finally:
        cs.close()
        ctx.close()

if myerror == 0:
    # Send log file email
    send_email_via_snowflake(
        subject=f"Cervix Log from {datetime.now().strftime('%Y-%m-%d')}",
        body=f"This is the cervix run log file for {datetime.now().strftime('%Y-%m-%d')}.",
        to_emails=["angela.eckstrand@albertahealthservices.ca", "tuyet.thieu@albertahealthservices.ca"]
    )

    # Send results email
    send_email_via_snowflake(
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
    send_email_via_snowflake(
        subject=f"ERRORS in Cervix run {datetime.now().strftime('%Y-%m-%d')} 2014-009",
        body=f"The cervix run log of {datetime.now().strftime('%Y-%m-%d')} contained errors. No data was sent. The log file is attached.",
        to_emails=["angela.eckstrand@albertahealthservices.ca", "tuyet.thieu@albertahealthservices.ca"]
    )