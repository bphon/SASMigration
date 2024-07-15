import pandas as pd
import numpy as np

# Load the initial dataset
df = pd.read_csv('Task5\sampledata.csv')

# Clean and prepare the data
df['path_t'] = df['ajcc8_path_t'].str[1:].str.strip()
df['path_n'] = df['ajcc8_path_n'].str[1:].str.strip()
df['path_m'] = df['ajcc8_path_m'].str[1:].str.strip()
df['path_stage'] = df['ajcc8_path_stage'].str.strip()

df['clin_t'] = df['ajcc8_clinical_t'].str[1:].str.strip()
df['clin_n'] = df['ajcc8_clinical_n'].str[1:].str.strip()
df['clin_m'] = df['ajcc8_clinical_m'].str[1:].str.strip()
df['clin_stage'] = df['ajcc8_clinical_stage'].str.strip()

df['post_t'] = df['ajcc8_postTherapy_t'].str[2:].str.strip()
df['post_n'] = df['ajcc8_postTherapy_n'].str[2:].str.strip()
df['post_m'] = df['ajcc8_postTherapy_m'].str[1:].str.strip()
df['post_stage'] = df['ajcc8_postTherapy_stage'].str.strip()

df['agegrp'] = np.where(df['age_diag'] < 55, '<55', '>=55')

df['PBLOODINVO'] = df['PERIPHERAL_BLOOD_INVO'].str.strip()
df['PBLOODINVO'] = df['PBLOODINVO'].replace('.', '')

df['ajcc8_id_new'] = df['ajcc8_id']

# Format PSA
def format_PSA(psa):
    if psa.upper() in ['XXX.1', 'XXX.2', 'XXX.3', 'XXX.7', 'XXX.9']:
        return np.nan
    else:
        return float(psa)

df['PSA_f'] = df['PSA'].apply(format_PSA)
