import pandas as pd

# Load the sample data
sample_data = pd.read_csv(r'g:\OneDrive\Desktop\AHS Work\Task6\sampledata.csv')

# Load the reference table
reference_tables = pd.read_excel(r'g:\OneDrive\Desktop\AHS Work\Task6\Reference Tables - Overall Stage Group Long 2023.xlsx', sheet_name=None)

# Print the columns to verify their names
print("Sample Data Columns:", sample_data.columns)

# Clean and preprocess the data
columns_to_strip = {
    'ajcc8_path_t': 'path_t',
    'ajcc8_path_n': 'path_n',
    'ajcc8_path_m': 'path_m',
    'ajcc8_path_Stage': 'path_stage',
    'ajcc8_clinical_t': 'clin_t',
    'ajcc8_clinical_n': 'clin_n',
    'ajcc8_clinical_m': 'clin_m',
    'ajcc8_clinical_Stage': 'clin_stage',
    'ajcc8_postTherapy_t': 'post_t',
    'ajcc8_postTherapy_n': 'post_n',
    'ajcc8_postTherapy_m': 'post_m',
    'ajcc8_PostTherapy_Stage': 'post_stage'
}

for old_col, new_col in columns_to_strip.items():
    if old_col in sample_data.columns:
        sample_data[new_col] = sample_data[old_col].astype(str).str.strip()

# Define a function to check invalid stages
def check_invalid_stage(row, reference_df, stage_type):
    t_value = row.get(f'{stage_type}_t', '')
    n_value = row.get(f'{stage_type}_n', '')
    m_value = row.get(f'{stage_type}_m', '')
    stage = row.get(f'{stage_type}_stage', '')
    
    matched = reference_df[(reference_df['t_value'] == t_value) & 
                           (reference_df['n_value'] == n_value) & 
                           (reference_df['m_value'] == m_value)]
    
    if not matched.empty and stage not in matched['stage_group'].values:
        return True
    return False

# Iterate over the reference tables and sample data to identify invalid records
invalid_records = []

for schema_id, ref_df in reference_tables.items():
    ref_df.columns = ref_df.columns.str.lower()
    for index, row in sample_data.iterrows():
        if row['schema_Id'] == schema_id:
            for stage_type in ['clin', 'path', 'post']:
                if check_invalid_stage(row, ref_df, stage_type):
                    invalid_records.append(row)

invalid_df = pd.DataFrame(invalid_records)

# Output the invalid records to an Excel file
invalid_df.to_excel(r'g:\OneDrive\Desktop\AHS Work\Task6\Invalid_StageGroup_Edits.xlsx', index=False)
