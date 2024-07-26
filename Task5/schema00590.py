import pandas as pd
from sqlalchemy import create_engine

# Define your database connection object
# conn = your_database_connection_object

# Step 1: Import data from Excel
sg_00730 = pd.read_excel("path_to_your_excel_file.xlsx", sheet_name="TNM_00730")

# Step 2: Sort and remove duplicates
sg_00730_sorted = sg_00730.drop_duplicates(subset=['schemaid', 't_value', 'n_value', 'm_value', 'descriptor', 'ajccid', 'agegroup'])

# Step 3: SQL Query to create the Invalid_00730 table
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
JOIN sg_00730_sorted b ON a.schema_id = b.schemaid
WHERE
    (
        (b.descriptor IN ('c', 'cp') AND
        (
            (b.t_value = 'T' AND a.clin_t LIKE '%') OR a.clin_t LIKE b.t_value
        ) AND 
        (
            (b.n_value = 'N' AND a.clin_n LIKE '%') OR a.clin_n LIKE b.n_value
        ) AND 
        (
            (b.m_value = 'M' AND a.clin_m LIKE '%') OR a.clin_m LIKE b.m_value
        ) AND 
        (a.ajcc8_id = b.ajccid) AND 
        (
            (b.agegroup = 'A' AND a.agegrp LIKE '%') OR b.agegroup = a.agegrp
        ) AND 
        a.clin_stage != b.stage_group
    )
    OR 
    (
        b.descriptor IN ('p', 'cp') AND 
        (
            (b.t_value = 'T' AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value
        ) AND 
        (
            (b.n_value = 'N' AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value
        ) AND 
        (
            (b.m_value = 'M' AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value
        ) AND 
        (a.ajcc8_id = b.ajccid) AND 
        (
            (b.agegroup = 'A' AND a.agegrp LIKE '%') OR b.agegroup = a.agegrp
        ) AND 
        a.path_stage != b.stage_group
    )
    OR 
    (
        b.descriptor IN ('p', 'cp') AND a.ajcc8_path_stage = ' ' AND
        (
            (b.t_value = 'T' AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value
        ) AND 
        (
            (b.n_value = 'N' AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value
        ) AND 
        (
            (b.m_value = 'M' AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value
        ) AND 
        (a.ajcc8_id = b.ajccid) AND 
        (
            (b.agegroup = 'A' AND a.agegrp LIKE '%') OR b.agegroup = a.agegrp
        ) AND 
        a.post_stage != b.stage_group
    )
)
ORDER BY a.schema_id
"""

# Execute the query and store the result in a DataFrame
Invalid_00730 = pd.read_sql_query(query, con=conn)

# Display the DataFrame
print(Invalid_00730)