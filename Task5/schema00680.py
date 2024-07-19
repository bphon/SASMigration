# STAGE GROUP IS DIFFERENT FOR CLINICAL AND PATHOLOGICAL STAGE GROUP
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

    def process_sg_TNM00680(self):
        sg_TNM00680_sorted = self.import_and_sort_data("TNM_00680", ['schemaid', 't_value', 'n_value', 'm_value', 'descriptor'])

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
        JOIN sg_TNM00680_sorted b ON a.schema_id = b.schemaid
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
                a.clin_stage != b.stage_group
            )
            OR 
            (
                b.descriptor IN ('p', 'cp') AND 
                (
                    (b.t_value = 'T' AND a.ajcc8_path_t LIKE '%') OR a.ajcc8_path_t LIKE b.t_value
                ) AND 
                (
                    (b.n_value = 'N' AND a.ajcc8_path_n LIKE '%') OR a.ajcc8_path_n LIKE b.n_value
                ) AND 
                (
                    (b.m_value = 'M' AND a.ajcc8_path_m LIKE '%') OR a.ajcc8_path_m LIKE b.m_value
                ) AND 
                a.path_stage != b.stage_group
            )
            OR 
            (
                b.descriptor IN ('p', 'cp') AND a.ajcc8_path_stage = ' ' AND
                (
                    (b.t_value = 'T' AND a.ajcc8_posttherapy_t LIKE '%') OR a.ajcc8_posttherapy_t LIKE b.t_value
                ) AND 
                (
                    (b.n_value = 'N' AND a.ajcc8_posttherapy_n LIKE '%') OR a.ajcc8_posttherapy_n LIKE b.n_value
                ) AND 
                (
                    (b.m_value = 'M' AND a.ajcc8_posttherapy_m LIKE '%') OR a.ajcc8_posttherapy_m LIKE b.m_value
                ) AND 
                a.post_stage != b.stage_group
            )
        )
        ORDER BY a.schema_id
        """

        return self.execute_query(query)

# Usage example
tnm_processor = TNMProcessor("SASMigration\Task5\Reference Tables - Overall Stage Group Long 2023.xlsx", 'your_database_connection_string')
invalid_stage_00680 = tnm_processor.process_sg_TNM00680()

print(invalid_stage_00680)
