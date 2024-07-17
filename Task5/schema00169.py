# SCHEMA 00169 Esophagus (including GE junction) (excluding Squamous)
# THIS SCHEMAS USE GRADE WHEN DECIDING ON STAGE GROUP FOR PATH AND POSTTHERAPY
# PATH AND POSTTHERAPY STAGE TABLES DIFFER

import pandas as pd
from sqlalchemy import create_engine

# Assuming you have a defined connection object named `conn`
# For example, conn = create_engine('your_database_connection_string').connect()

# Step 1: Import data from Excel
tnm00169p = pd.read_excel("SASMigration\Task5\Reference Tables - Overall Stage Group Long 2023.xlsx", sheet_name="TNM_00169_path")

# Step 2: Sort and remove duplicates
tnm00169p_sorted = tnm00169p.drop_duplicates(subset=['schemaid', 't_value', 'n_value', 'm_value', 'grade', 'descriptor'])

# Step 3: SQL Query to create the Invalid_Stage_00169 table
query = """
SELECT DISTINCT
    a.acb_no,
    a.mal_no, 
    a.malignancy_id, 
    a.person_id,
    a.diagnosis_date, 
    a.agegrp,
    a.age_diag, 
    a.icdo_top, 
    a.icdo_mor, 
    a.inc_site_fine_text, 
    a.f_loc, 
    a.mal_comment, 
    a.schema_id, 
    a.ajcc8_id,
    a.discriminator2,
    a.clin_t, 
    a.clin_n, 
    a.clin_m, 
    a.clin_stage,
    a.path_t, 
    a.path_n, 
    a.path_m, 
    a.path_stage,
    a.post_t, 
    a.post_n, 
    a.post_m, 
    a.post_stage,
    a.ajcc8_clinical_grade, 
    a.ajcc8_path_grade, 
    a.ajcc8_posttherapy_grade, 
    a.s_category_clin, 
    a.s_category_path, 
    a.HER2_SUMMARY, 
    a.ER, 
    a.PR, 
    a.PSA, 
    a.PBLOODINVO,
    a.psa_f,
    1 AS tnm_edit2000
FROM Step1B_TNMedits a
JOIN tnm00169p_sorted b ON a.schema_id = b.schemaid
WHERE
    (
        (b.descriptor IN ('p', 'cp') AND 
        (
            (b.t_value = 'T' AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value
        ) AND 
        (
            (b.n_value = 'N' AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value
        ) AND 
        (
            (b.m_value = 'M' AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value
        ) AND 
        (
            (b.grade = 'G' AND a.ajcc8_path_grade LIKE '%') OR a.ajcc8_path_grade LIKE b.grade
        ) AND 
        a.path_stage != b.stage_group
    )
    OR 
    (
        b.descriptor = 'y' AND a.ajcc8_path_stage = ' ' AND
        (
            (b.t_value = 'T' AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value
        ) AND 
        (
            (b.n_value = 'N' AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value
        ) AND 
        (
            (b.m_value = 'M' AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value
        ) AND 
        (
            (b.grade = 'G' AND a.ajcc8_posttherapy_grade LIKE '%') OR a.ajcc8_posttherapy_grade LIKE b.grade
        ) AND 
        a.post_stage != b.stage_group
    )
)
ORDER BY a.schema_id
"""

# Execute the query and store the result in a DataFrame
Invalid_Stage_00169 = pd.read_sql_query(query, con=conn)

# Display the DataFrame
print(Invalid_Stage_00169)
