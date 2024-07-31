import pandas as pd

file_path = 'G:\\OneDrive\\Desktop\\AHS Work\\SASMigration\\Task5\\Reference Tables - Overall Stage Group Long 2023.xlsx'

try:
    # Use openpyxl engine to read the Excel file
    df = pd.read_excel(file_path, engine='openpyxl')
    print("File loaded successfully.")
except Exception as e:
    print(f"Error loading data from {file_path}: {e}")

# Further processing of the data if needed