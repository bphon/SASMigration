# SCHEMA INCLUDES HER2, ER, PR, GRADE WHEN DECIDING ON STAGE GROUP 
# ALSO FOR CASES WITH ONCOTYPE_DX_SCORE<11 THERE IS A DIFFERENT TABLE FOR PATHOLOGICAL STAGE GROUP 

import pandas as pd
from sqlalchemy import create_engine

class TNMProcessor:
    def __init__(self, excel_file, db_connection_string):
        self.excel_file = excel_file
        self.engine = create_engine(db_connection_string)

    def import_and_sort_data(self, sheet_name, sort_columns):
        df = pd.read_excel(self.excel_file, sheet_name=sheet_name)
        sorted_df = df.drop_duplicates(subset=sort_columns)
        return sorted_df

    def execute_query(self, query):
        return pd.read_sql_query(query, con=self.engine)

    def process_sg_00480(self):
        sg_00480_sorted = self.import_and_sort_data("TNM_00480old", ['schemaid', 't_value', 'n_value', 'm_value', 'descriptor', 'ER_value', 'PR_value', 'HER2_SUM_Value', 'grade'])
        
        query1 = """
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
            a.ONCOTYPE_DX_SCORE,
            a.s_category_clin, 
            a.s_category_path, 
            a.HER2_SUMMARY, 
            a.ER, 
            a.PR, 
            a.PSA, 
            a.PBLOODINVO,
            a.psa_f,
            a.brcomflg,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00480_sorted b ON a.schema_id = b.schemaid
        WHERE
            (
                (b.descriptor IN ('c', 'cp') AND
                (
                    (b.t_value = 'T' AND a.ajcc8_clinical_t LIKE '%') OR a.ajcc8_clinical_t LIKE b.t_value
                ) AND 
                (
                    (b.n_value = 'N' AND a.ajcc8_clinical_n LIKE '%') OR a.ajcc8_clinical_n LIKE b.n_value
                ) AND 
                (
                    (b.m_value = 'M' AND a.ajcc8_clinical_m LIKE '%') OR a.ajcc8_clinical_m LIKE b.m_value
                ) AND 
                (a.ER = b.ER_value) AND 
                (a.PR = b.PR_value) AND 
                (a.HER2_SUMMARY = b.HER2_SUM_Value) AND 
                (a.ajcc8_clinical_grade = b.grade) AND 
                a.ajcc8_clinical_stage != b.stage_group
            )
            OR 
            (
                b.descriptor IN ('p', 'cp') AND 
                (a.ONCOTYPE_DX_SCORE >= '011' AND a.ONCOTYPE_DX_SCORE NOT IN ('XX4')) AND
                (
                    (b.t_value = 'T' AND a.ajcc8_path_t LIKE '%') OR a.ajcc8_path_t LIKE b.t_value
                ) AND 
                (
                    (b.n_value = 'N' AND a.ajcc8_path_n LIKE '%') OR a.ajcc8_path_n LIKE b.n_value
                ) AND 
                (
                    (b.m_value = 'M' AND a.ajcc8_path_m LIKE '%') OR a.ajcc8_path_m LIKE b.m_value
                ) AND 
                (a.ER = b.ER_value) AND 
                (a.PR = b.PR_value) AND 
                (a.HER2_SUMMARY = b.HER2_SUM_Value) AND 
                (a.ajcc8_path_grade = b.grade) AND 
                a.ajcc8_path_stage != b.stage_group
            )
        )
        ORDER BY a.schema_id
        """

        return self.execute_query(query1)

    def process_sg_00480_onc(self):
        sg_00480_onc_sorted = self.import_and_sort_data("TNM_00480_ONC11", ['schemaid', 't_value', 'n_value', 'm_value', 'descriptor', 'ER_value', 'HER2_SUM_Value'])

        query2 = """
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
            a.ONCOTYPE_DX_SCORE,
            a.s_category_clin, 
            a.s_category_path, 
            a.HER2_SUMMARY, 
            a.ER, 
            a.PR, 
            a.PSA, 
            a.PBLOODINVO,
            a.psa_f,
            a.brcomflg,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00480_onc_sorted b ON a.schema_id = b.schemaid
        WHERE
            b.descriptor IN ('p', 'cp') AND 
            (a.ONCOTYPE_DX_SCORE BETWEEN '000' AND '010' OR a.ONCOTYPE_DX_SCORE IN ('XX4')) AND
            (
                (b.t_value = 'T' AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value
            ) AND 
            (
                (b.n_value = 'N' AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value
            ) AND 
            (
                (b.m_value = 'M' AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value
            ) AND 
            (a.ER = b.ER_value) AND 
            (a.HER2_SUMMARY = b.HER2_SUM_Value) AND 
            a.ajcc8_path_stage != b.stage_group
        ORDER BY a.schema_id
        """

        return self.execute_query(query2)

# Usage example
tnm_processor = TNMProcessor("SASMigration\Task5\Reference Tables - Overall Stage Group Long 2023.xlsx", 'your_database_connection_string')
invalid_stage_00480 = tnm_processor.process_sg_00480()
invalid_stage_00480_onc = tnm_processor.process_sg_00480_onc()

print(invalid_stage_00480)
print(invalid_stage_00480_onc)
