import pandas as pd

file_path = r'G:\OneDrive\Desktop\AHS Work\SASMigration\Task5\testing.xlsx'
try:
    data = pd.read_excel(file_path, sheet_name='TNM_00590', engine='openpyxl')
    print(data.head())
except Exception as e:
    print(f"Error reading the file: {e}")
