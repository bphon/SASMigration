import pandas as pd

# Assuming you have a defined connection object named `conn`
# For example, conn = create_engine('your_database_connection_string').connect()

# Query to create the Invalid_SG_allschemas table
query1 = """
SELECT DISTINCT
    acb_no,
    mal_no, 
    malignancy_id, 
    person_id,
    diagnosis_date, 
    agegrp,
    age_diag, 
    icdo_top, 
    icdo_mor, 
    inc_site_fine_text, 
    f_loc, 
    mal_comment, 
    schema_id, 
    ajcc8_id,
    discriminator2,
    clin_t, 
    clin_n, 
    clin_m, 
    clin_stage,
    path_t, 
    path_n, 
    path_m, 
    path_stage,
    post_t, 
    post_n, 
    post_m, 
    post_stage,
    ajcc8_clinical_grade, 
    ajcc8_path_grade, 
    ajcc8_posttherapy_grade, 
    s_category_clin, 
    s_category_path, 
    HER2_SUMMARY, 
    ER, 
    PR, 
    a.PSA, 
    PBLOODINVO,
    psa_f,
    1 AS tnm_edit2000
FROM Step1B_TNMedits a   
WHERE 
    (ajcc8_path_t = ' ' AND ajcc8_path_n = ' ' AND ajcc8_path_m = ' ' AND ajcc8_path_stage NOT IN ('99', '88', ' ')) 
    OR 
    (ajcc8_clinical_t = ' ' AND ajcc8_clinical_n = ' ' AND ajcc8_clinical_m = ' ' AND ajcc8_clinical_stage NOT IN ('99', '88'))
    OR 
    (ajcc8_posttherapy_t = ' ' AND ajcc8_posttherapy_n = ' ' AND ajcc8_posttherapy_m = ' ' AND ajcc8_posttherapy_stage NOT IN ('99', '88', ' '))
ORDER BY schema_id
"""

# Query to create the Invalid_SG_allschemas2 table
query2 = """
SELECT DISTINCT
    acb_no,
    mal_no, 
    malignancy_id, 
    person_id,
    diagnosis_date, 
    agegrp,
    age_diag, 
    icdo_top, 
    icdo_mor, 
    inc_site_fine_text, 
    f_loc, 
    mal_comment, 
    schema_id, 
    ajcc8_id,
    discriminator2,
    clin_t, 
    clin_n, 
    clin_m, 
    clin_stage,
    path_t, 
    path_n, 
    path_m, 
    path_stage,
    post_t, 
    post_n, 
    post_m, 
    post_stage,
    ajcc8_clinical_grade, 
    ajcc8_path_grade, 
    ajcc8_posttherapy_grade, 
    s_category_clin, 
    s_category_path, 
    HER2_SUMMARY, 
    ER, 
    PR, 
    a.PSA, 
    PBLOODINVO,
    psa_f,
    1 AS tnm_edit2000
FROM Step1B_TNMedits a   
WHERE
    schema_id NOT IN ('00730') AND 
    (
        (ajcc8_path_t = ' ' AND ajcc8_path_n = ' ' AND path_m = 'M0' AND ajcc8_path_stage NOT IN ('99', '88')) 
        OR 
        (ajcc8_clinical_t = ' ' AND ajcc8_clinical_n = ' ' AND clin_m = 'M0' AND ajcc8_clinical_stage NOT IN ('99', '88'))
        OR 
        (ajcc8_posttherapy_t = ' ' AND ajcc8_posttherapy_n = ' ' AND post_m = 'M0' AND ajcc8_posttherapy_stage NOT IN ('99', '88'))
    )
ORDER BY schema_id
"""

# Execute the queries and store the results in DataFrames
# Replace conn with database connection object
Invalid_SG_allschemas = pd.read_sql_query(query1, con=conn)
Invalid_SG_allschemas2 = pd.read_sql_query(query2, con=conn)

# Display the DataFrames
print(Invalid_SG_allschemas)
print(Invalid_SG_allschemas2)
