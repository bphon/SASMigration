import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.sql import text
import smtplib
from email.message import EmailMessage
import oracledb
import os
import zipfile


class TNMEdits:
    def __init__(self):
        self.df = None # dataframe to hold the data 
        self.engine = None  # Update this with your actual connection string

    def load_data(self, file_path):
        if os.path.exists(file_path):
            try:
                self.df = pd.read_csv(file_path)
                if self.df.empty:
                    print(f"The file {file_path} is empty.")
                else:
                    print("Columns in the loaded DataFrame:", self.df.columns)
            except Exception as e:
                print(f"Error loading data from {file_path}: {e}")
        else:
            print(f"File not found: {file_path}")

    def load_sg_data(self, file_path, sheet_name=None):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        if os.path.getsize(file_path) == 0:
            print(f"The file {file_path} is empty.")
            return None

        try:
            if file_path.endswith('.xlsx'):
                sg_data = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
            elif file_path.endswith('.xls'):
                sg_data = pd.read_excel(file_path, sheet_name=sheet_name, engine='xlrd')
            elif file_path.endswith('.csv'):
                sg_data = pd.read_csv(file_path)
            else:
                raise ValueError("Unsupported file type")

            if sg_data.empty:
                print("The file contains no data.")
                return None
            
            sg_data = sg_data.drop_duplicates()
            return sg_data
        except Exception as e:
            print(f"An error occurred while loading the schema data: {e}")
        return None

    def step1b_tnmedits(self):
        required_columns = [
            'ajcc8_path_t', 'ajcc8_path_n', 'ajcc8_path_m', 'ajcc8_path_stage',
            'ajcc8_clinical_t', 'ajcc8_clinical_n', 'ajcc8_clinical_m', 'ajcc8_clinical_stage',
            'ajcc8_postTherapy_t', 'ajcc8_postTherapy_n', 'ajcc8_postTherapy_m', 'ajcc8_postTherapy_stage',
            'age_diag', 'PERIPHERAL_BLOOD_INVO', 'ajcc8_id', 'PSA'
        ]

        # Assign default values for missing columns
        for column in required_columns:
            if column not in self.df.columns:
                self.df[column] = ''

        if 'ajcc8_path_t' in self.df.columns:
            self.df['path_t'] = self.df['ajcc8_path_t'].str[1:].str.strip()
        if 'ajcc8_path_n' in self.df.columns:
            self.df['path_n'] = self.df['ajcc8_path_n'].str[1:].str.strip()
        if 'ajcc8_path_m' in self.df.columns:
            self.df['path_m'] = self.df['ajcc8_path_m'].str[1:].str.strip()
        if 'ajcc8_path_stage' in self.df.columns:
            self.df['path_stage'] = self.df['ajcc8_path_stage'].str.strip()

        if 'ajcc8_clinical_t' in self.df.columns:
            self.df['clin_t'] = self.df['ajcc8_clinical_t'].str[1:].str.strip()
        if 'ajcc8_clinical_n' in self.df.columns:
            self.df['clin_n'] = self.df['ajcc8_clinical_n'].str[1:].str.strip()
        if 'ajcc8_clinical_m' in self.df.columns:
            self.df['clin_m'] = self.df['ajcc8_clinical_m'].str[1:].str.strip()
        if 'ajcc8_clinical_stage' in self.df.columns:
            self.df['clin_stage'] = self.df['ajcc8_clinical_stage'].str.strip()

        if 'ajcc8_postTherapy_t' in self.df.columns:
            self.df['post_t'] = self.df['ajcc8_postTherapy_t'].str[2:].str.strip()
        if 'ajcc8_postTherapy_n' in self.df.columns:
            self.df['post_n'] = self.df['ajcc8_postTherapy_n'].str[2:].str.strip()
        if 'ajcc8_postTherapy_m' in self.df.columns:
            self.df['post_m'] = self.df['ajcc8_postTherapy_m'].str[1:].str.strip()
        if 'ajcc8_postTherapy_stage' in self.df.columns:
            self.df['post_stage'] = self.df['ajcc8_postTherapy_stage'].str.strip()

        if 'age_diag' in self.df.columns:
            self.df['age_diag'] = pd.to_numeric(self.df['age_diag'], errors='coerce')  # Convert to numeric
            self.df['agegrp'] = self.df['age_diag'].apply(lambda x: "<55" if x < 55 else ">=55")

        if 'PERIPHERAL_BLOOD_INVO' in self.df.columns:
            self.df['PBLOODINVO'] = self.df['PERIPHERAL_BLOOD_INVO'].str.strip().replace('.', '')

        if 'ajcc8_id' in self.df.columns:
            self.df['ajcc8_id_new'] = self.df['ajcc8_id']

        if 'PSA' in self.df.columns:
            self.format_PSA()

    def format_PSA(self):
        self.df['PSA_f'] = self.df['PSA'].apply(lambda x: None if x in ['XXX.1', 'XXX.2', 'XXX.3', 'XXX.7', 'XXX.9'] else float(x))

    def invalid_stage_grade(self, sg_grade):
        invalid_stage_grade = self.df.merge(
            sg_grade,
            how='inner',
            left_on='schema_id',
            right_on='schemaid'
        ).query(
            "(descriptor in ['c', 'cp'] and ((t_value == 'T' and clin_t.str.contains('%')) or clin_t.str.contains(t_value)) and ((n_value == 'N' and clin_n.str.contains('%')) or clin_n.str.contains(n_value)) and ((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and ((grade == 'G' and ajcc8_clinical_grade.str.contains('%')) or ajcc8_clinical_grade.str.contains(grade)) and clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and ((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and ((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and ((grade == 'G' and ajcc8_path_grade.str.contains('%')) or ajcc8_path_grade.str.contains(grade)) and path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and ((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and ((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and ((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and ((grade == 'G' and ajcc8_posttherapy_grade.str.contains('%')) or ajcc8_posttherapy_grade.str.contains(grade)) and post_stage != stage_group)"
        )
        invalid_stage_grade['tnm_edit2000'] = 1
        return invalid_stage_grade

    #TNM Overall Staging Edits
    def invalid_sg_allschemas(self):
        query = """
        SELECT DISTINCT 
            acb_no, mal_no, malignancy_id, person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            schema_id, ajcc8_id, discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits
        WHERE 
            (ajcc8_path_t=' ' AND ajcc8_path_n=' ' AND ajcc8_path_m=' ' AND ajcc8_path_stage NOT IN ('99', '88', ' ')) 
            OR (ajcc8_clinical_t=' ' AND ajcc8_clinical_n=' ' AND ajcc8_clinical_m=' ' AND ajcc8_clinical_stage NOT IN ('99', '88'))
            OR (ajcc8_posttherapy_t=' ' AND ajcc8_posttherapy_n=' ' AND ajcc8_posttherapy_m=' ' AND ajcc8_posttherapy_stage NOT IN ('99', '88', ' '))
        ORDER BY schema_id
        """
        invalid_sg_allschemas = pd.read_sql_query(text(query), self.engine.connect())
        return invalid_sg_allschemas

    def invalid_sg_allschemas2(self):
        query = """
        SELECT DISTINCT 
            acb_no, mal_no, malignancy_id, person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            schema_id, ajcc8_id, discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits
        WHERE 
            schema_id NOT IN ('00730') AND 
            ((ajcc8_path_t=' ' AND ajcc8_path_n=' ' AND path_m='M0' AND ajcc8_path_stage NOT IN ('99', '88')) 
            OR (ajcc8_clinical_t=' ' AND ajcc8_clinical_n=' ' AND clin_m='M0' AND ajcc8_clinical_stage NOT IN ('99', '88'))
            OR (ajcc8_posttherapy_t=' ' AND ajcc8_posttherapy_n=' ' AND post_m='M0' AND ajcc8_posttherapy_stage NOT IN ('99', '88')))
        ORDER BY schema_id
        """
        invalid_sg_allschemas2 = pd.read_sql_query(text(query), self.engine)
        return invalid_sg_allschemas2

    def invalid_schema_00811(self, sg_00811):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00811 b ON a.schema_id = b.SchemaId
        WHERE 
            b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.clin_t LIKE '%') OR a.clin_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.clin_n LIKE '%') OR a.clin_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.clin_m LIKE '%') OR a.clin_m LIKE b.m_value) AND
            a.PBLOODINVO = b.p_blood_value AND
            a.clin_stage <> b.stage_group
        ORDER BY a.schema_id
        """
        invalid_00811 = pd.read_sql_query(text(query), self.engine)
        return invalid_00811

    def invalid_schema_00580(self, sg_00580):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f, b.stage_group AS clinical_stage_grp,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00580 b ON a.schema_id = b.SchemaId
        WHERE 
            ((b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.ajcc8_clinical_t LIKE '%') OR a.ajcc8_clinical_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.clin_n LIKE '%') OR a.clin_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.clin_m LIKE '%') OR a.clin_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_clinical_grade LIKE '%') OR a.ajcc8_clinical_grade LIKE b.grade) AND
            ((b.PSA_X IN ('XXX.1') AND a.PSA = b.PSA_X) OR (b.PSA = 'Any' AND a.PSA LIKE '%') OR (b.low <= a.PSA_f <= b.high)) AND
            a.clin_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T') AND a.ajcc8_path_t LIKE '%') OR a.ajcc8_path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_path_grade LIKE '%') OR a.ajcc8_path_grade LIKE b.grade) AND
            ((b.PSA_X IN ('XXX.1') AND a.PSA = b.PSA_X) OR (b.PSA = 'Any' AND a.PSA LIKE '%') OR (b.low <= a.PSA_f <= b.high)) AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T') AND a.ajcc8_posttherapy_t LIKE '%') OR a.ajcc8_posttherapy_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_posttherapy_grade LIKE '%') OR a.ajcc8_posttherapy_grade LIKE b.grade) AND
            ((b.PSA_X IN ('XXX.1') AND a.PSA = b.PSA_X) OR (b.PSA = 'Any' AND a.PSA LIKE '%') OR (b.low <= a.PSA_f <= b.high)) AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_00580 = pd.read_sql_query(text(query), self.engine)
        return invalid_00580

    def invalid_schema_00730(self, sg_00730):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00730 b ON a.schema_id = b.SchemaId
        WHERE 
            ((b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.clin_t LIKE '%') OR a.clin_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.clin_n LIKE '%') OR a.clin_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.clin_m LIKE '%') OR a.clin_m LIKE b.m_value) AND
            a.ajcc8_id = b.ajccid AND
            ((b.agegroup IN ('A') AND a.agegrp LIKE '%') OR b.agegroup = a.agegrp)) AND
            a.clin_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T') AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            a.ajcc8_id = b.ajccid AND
            ((b.agegroup IN ('A') AND a.agegrp LIKE '%') OR b.agegroup = a.agegrp) AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T') AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value) AND
            a.ajcc8_id = b.ajccid AND
            ((b.agegroup IN ('A') AND a.agegrp LIKE '%') OR b.agegroup = a.agegrp) AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_00730 = pd.read_sql_query(text(query), self.engine)
        return invalid_00730
        

    def invalid_schema_00590(self, sg_00590):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00590 b ON a.schema_id = b.SchemaId
        WHERE 
            ((b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.clin_t LIKE '%') OR a.clin_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.clin_n LIKE '%') OR a.clin_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.clin_m LIKE '%') OR a.clin_m LIKE b.m_value) AND
            ((b.s_cat IN ('S') AND a.s_category_clin LIKE '%') OR a.s_category_clin LIKE b.s_cat) AND
            a.clin_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T', 'pT') AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            ((b.s_cat IN ('S') AND a.s_category_path LIKE '%') OR a.s_category_path LIKE b.s_cat) AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T', 'pT') AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value) AND
            ((b.s_cat IN ('S') AND a.s_category_path LIKE '%') OR a.s_category_path LIKE b.s_cat) AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_00590 = pd.read_sql_query(text(query), self.engine)
        return invalid_00590

    def invalid_schema_00111(self, sg_00111):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00111 b ON a.schema_id = b.SchemaId
        WHERE 
            ((b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.clin_t LIKE '%') OR a.clin_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.clin_n LIKE '%') OR a.clin_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.clin_m LIKE '%') OR a.clin_m LIKE b.m_value) AND
            a.discriminator2 LIKE b.discriminator2 AND
            a.clin_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T') AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            a.discriminator2 LIKE b.discriminator2 AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T') AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value) AND
            a.discriminator2 LIKE b.discriminator2 AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_00111 = pd.read_sql_query(text(query), self.engine)
        return invalid_00111

    def invalid_schema_00560(self, sg_00560):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00560 b ON a.schema_id = b.SchemaId
        WHERE 
            ((b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.clin_t LIKE '%') OR a.clin_t LIKE b.t_value) AND
            ((b.m_value IN ('M') AND a.clin_m LIKE '%') OR a.clin_m LIKE b.m_value) AND
            a.gestational_prog_index = b.gtpi AND
            a.clin_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T') AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            a.gestational_prog_index = b.gtpi AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T') AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value) AND
            ((b.m_value IN ('M') AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value) AND
            a.gestational_prog_index = b.gtpi AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_00560 = pd.read_sql_query(text(query), self.engine)
        return invalid_00560

    def invalid_schema_00430(self, sg_00430):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00430 b ON a.schema_id = b.SchemaId
        WHERE 
            ((b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.clin_t LIKE '%') OR a.clin_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.clin_n LIKE '%') OR a.clin_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.clin_m LIKE '%') OR a.clin_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_clinical_grade LIKE '%') OR a.ajcc8_clinical_grade LIKE b.grade) AND
            a.ajcc8_id = b.ajccid AND
            a.clin_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T') AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_path_grade LIKE '%') OR a.ajcc8_path_grade LIKE b.grade) AND
            a.ajcc8_id = b.ajccid AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T') AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_posttherapy_grade LIKE '%') OR a.ajcc8_posttherapy_grade LIKE b.grade) AND
            a.ajcc8_id = b.ajccid AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_00430 = pd.read_sql_query(text(query), self.engine)
        return invalid_00430

    def invalid_schema_00169(self, sg_00169):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00169 b ON a.schema_id = b.SchemaId
        WHERE 
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T') AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_path_grade LIKE '%') OR a.ajcc8_path_grade LIKE b.grade) AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('y') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T') AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_posttherapy_grade LIKE '%') OR a.ajcc8_posttherapy_grade LIKE b.grade) AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_00169 = pd.read_sql_query(text(query), self.engine)
        return invalid_00169
    def invalid_stage_grade(self, sg_grade):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, a.diagnosis_date, a.agegrp, a.age_diag, a.icdo_top, a.icdo_mor, a.inc_site_fine_text, a.f_loc, a.mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, a.clin_t, a.clin_n, a.clin_m, a.clin_stage, a.path_t, a.path_n, a.path_m, a.path_stage,
            a.post_t, a.post_n, a.post_m, a.post_stage, a.ajcc8_clinical_grade, a.ajcc8_path_grade, a.ajcc8_posttherapy_grade, 
            a.s_category_clin, a.s_category_path, a.HER2_SUMMARY, a.ER, a.PR, a.PSA, a.PBLOODINVO, a.psa_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_grade b ON a.schema_id = b.schemaid
        WHERE 
            (
            (b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.clin_t LIKE '%') OR a.clin_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.clin_n LIKE '%') OR a.clin_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.clin_m LIKE '%') OR a.clin_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_clinical_grade LIKE '%') OR a.ajcc8_clinical_grade LIKE b.grade) AND
            a.clin_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T') AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_path_grade LIKE '%') OR a.ajcc8_path_grade LIKE b.grade) AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T') AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_posttherapy_grade LIKE '%') OR a.ajcc8_posttherapy_grade LIKE b.grade) AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_stage_grade = pd.read_sql_query(text(query), self.engine)
        return invalid_stage_grade

    def invalid_schema_00480(self, sg_00480):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, ONCOTYPE_DX_SCORE,
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f, brcomflg,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00480 b ON a.schema_id = b.SchemaId
        WHERE 
            (b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.ajcc8_clinical_t LIKE '%') OR a.ajcc8_clinical_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.ajcc8_clinical_n LIKE '%') OR a.ajcc8_clinical_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.ajcc8_clinical_m LIKE '%') OR a.ajcc8_clinical_m LIKE b.m_value) AND
            a.ER = b.ER_value AND
            a.PR = b.PR_value AND
            a.HER2_SUMMARY = b.HER2_SUM_Value AND
            a.ajcc8_clinical_grade = b.grade AND
            a.ajcc8_clinical_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND
            (a.ONCOTYPE_DX_SCORE >= '011' AND a.ONCOTYPE_DX_SCORE NOT IN ('XX4')) AND
            ((b.t_value IN ('T') AND a.ajcc8_path_t LIKE '%') OR a.ajcc8_path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.ajcc8_path_n LIKE '%') OR a.ajcc8_path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.ajcc8_path_m LIKE '%') OR a.ajcc8_path_m LIKE b.m_value) AND
            a.ER = b.ER_value AND
            a.PR = b.PR_value AND
            a.HER2_SUMMARY = b.HER2_SUM_Value AND
            a.ajcc8_path_grade = b.grade AND
            a.ajcc8_path_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_00480 = pd.read_sql_query(text(query), self.engine)
        return invalid_00480

    def invalid_schema_00480_onc(self, sg_00480_onc):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, ONCOTYPE_DX_SCORE,
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f, brcomflg,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00480_onc b ON a.schema_id = b.SchemaId
        WHERE 
            (b.descriptor IN ('p', 'cp') AND
            (a.ONCOTYPE_DX_SCORE < '011' OR a.ONCOTYPE_DX_SCORE IN ('XX4')) AND
            ((b.t_value IN ('T') AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            a.ER = b.ER_value AND
            a.HER2_SUMMARY = b.HER2_SUM_Value AND
            a.path_stage <> b.stage_group)
        ORDER BY a.schema_id
        """
        invalid_00480_onc = pd.read_sql_query(text(query), self.engine)
        return invalid_00480_onc
    
    def invalid_schema_00381_00440_00410_00190(self, sg_grade):
        invalid_schemas = self.df.merge(
            sg_grade,
            how='inner',
            left_on='schema_id',
            right_on='schemaid'
        ).query(
            "((descriptor in ['c', 'cp'] and ((t_value == 'T' and clin_t.str.contains('%')) or clin_t.str.contains(t_value)) and ((n_value == 'N' and clin_n.str.contains('%')) or clin_n.str.contains(n_value)) and ((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and ((grade == 'G' and ajcc8_clinical_grade.str.contains('%')) or ajcc8_clinical_grade.str.contains(grade)) and clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and ((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and ((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and ((grade == 'G' and ajcc8_path_grade.str.contains('%')) or ajcc8_path_grade.str.contains(grade)) and path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and ((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and ((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and ((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and ((grade == 'G' and ajcc8_posttherapy_grade.str.contains('%')) or ajcc8_posttherapy_grade.str.contains(grade)) and post_stage != stage_group))"
        )
        invalid_schemas['tnm_edit2000'] = 1
        return invalid_schemas

    def invalid_schema_00170(self, sg_00170):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00170 b ON a.schema_id = b.SchemaId
        WHERE 
            ((b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.clin_t LIKE '%') OR a.clin_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.clin_n LIKE '%') OR a.clin_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.clin_m LIKE '%') OR a.clin_m LIKE b.m_value) AND
            a.clin_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T') AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('y') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T') AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value) AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_00170 = pd.read_sql_query(text(query), self.engine)
        return invalid_00170

    def invalid_schema_00680(self, sg_00680):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00680 b ON a.schema_id = b.SchemaId
        WHERE 
            ((b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.ajcc8_clinical_t LIKE '%') OR a.ajcc8_clinical_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.ajcc8_clinical_n LIKE '%') OR a.ajcc8_clinical_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.ajcc8_clinical_m LIKE '%') OR a.ajcc8_clinical_m LIKE b.m_value) AND
            a.clin_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T') AND a.ajcc8_path_t LIKE '%') OR a.ajcc8_path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.ajcc8_path_n LIKE '%') OR a.ajcc8_path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.ajcc8_path_m LIKE '%') OR a.ajcc8_path_m LIKE b.m_value) AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T') AND a.ajcc8_posttherapy_t LIKE '%') OR a.ajcc8_posttherapy_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.ajcc8_posttherapy_n LIKE '%') OR a.ajcc8_posttherapy_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.ajcc8_posttherapy_m LIKE '%') OR a.ajcc8_posttherapy_m LIKE b.m_value) AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_00680 = pd.read_sql_query(text(query), self.engine)
        return invalid_00680

    def invalid_schema_00161(self, sg_00161):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, ESOPHAGUS_EGJ_TUMOR_CENTER, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_00161 b ON a.schema_id = b.SchemaId
        WHERE 
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T') AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_path_grade LIKE '%') OR a.ajcc8_path_grade LIKE b.grade) AND
            ((b.location IN ('L') AND a.ESOPHAGUS_EGJ_TUMOR_CENTER LIKE '%') OR a.ESOPHAGUS_EGJ_TUMOR_CENTER LIKE b.location) AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('y') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T') AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value) AND
            ((b.grade IN ('G') AND a.ajcc8_posttherapy_grade LIKE '%') OR a.ajcc8_posttherapy_grade LIKE b.grade) AND
            ((b.location IN ('L') AND a.ESOPHAGUS_EGJ_TUMOR_CENTER LIKE '%') OR a.ESOPHAGUS_EGJ_TUMOR_CENTER LIKE b.location) AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_00161 = pd.read_sql_query(text(query), self.engine)
        return invalid_00161

    def invalid_stage(self, sg_TNMonly):
        query = """
        SELECT DISTINCT 
            a.acb_no, a.mal_no, a.malignancy_id, a.person_id, diagnosis_date, agegrp, age_diag, icdo_top, icdo_mor, inc_site_fine_text, f_loc, mal_comment, 
            a.schema_id, a.ajcc8_id, a.discriminator2, clin_t, clin_n, clin_m, clin_stage, path_t, path_n, path_m, path_stage,
            post_t, post_n, post_m, post_stage, ajcc8_clinical_grade, ajcc8_path_grade, ajcc8_posttherapy_grade, 
            s_category_clin, s_category_path, HER2_SUMMARY, ER, PR, PSA, PBLOODINVO, PSA_f, b.stage_group AS path_stage_Grp,
            1 AS tnm_edit2000
        FROM Step1B_TNMedits a
        JOIN sg_TNMonly b ON a.schema_id = b.SchemaId
        WHERE 
            ((b.descriptor IN ('c', 'cp') AND
            ((b.t_value IN ('T') AND a.clin_t LIKE '%') OR a.clin_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.clin_n LIKE '%') OR a.clin_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.clin_m LIKE '%') OR a.clin_m LIKE b.m_value) AND
            a.clin_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND
            ((b.t_value IN ('T') AND a.path_t LIKE '%') OR a.path_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.path_n LIKE '%') OR a.path_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.path_m LIKE '%') OR a.path_m LIKE b.m_value) AND
            a.path_stage <> b.stage_group)
        OR
            (b.descriptor IN ('p', 'cp') AND a.ajcc8_path_stage = ' ' AND
            ((b.t_value IN ('T') AND a.post_t LIKE '%') OR a.post_t LIKE b.t_value) AND
            ((b.n_value IN ('N') AND a.post_n LIKE '%') OR a.post_n LIKE b.n_value) AND
            ((b.m_value IN ('M') AND a.post_m LIKE '%') OR a.post_m LIKE b.m_value) AND
            a.post_stage <> b.stage_group))
        ORDER BY a.schema_id
        """
        invalid_stage = pd.read_sql_query(text(query), self.engine)
        return invalid_stage

    def final_stagegroup_edits(self):
        sg_grade = self.load_sg_data(r'SASMigration\Task5\Reference Tables - Overall Stage Group Long 2023.xlsx', 'TNM_GRADE')
        invalid_stage_grade_df = self.invalid_stage_grade(sg_grade)
        
        other_invalid_data = pd.DataFrame()  # Replace with actual data loading and processing logic

        final_stagegroup_edits = pd.concat([
            invalid_stage_grade_df,
            other_invalid_data
        ])
        final_stagegroup_edits['tnm_edit_flag'] = "2000 Invalid Overall Stage Group for T/N/M Combo"
        return final_stagegroup_edits

    def export_to_excel(self, df, file_path, sheet_name):
        df.to_excel(file_path, sheet_name=sheet_name, index=False)


def main():
    tnm = TNMEdits()
    final_edits = None
    invalid_sg_allschemas = None
    invalid_sg_allschemas2 = None
    invalid_stage_grade_df = None
    invalid_00580 = None
    invalid_00730 = None
    invalid_00590 = None
    invalid_00111 = None
    invalid_00560 = None
    invalid_00430 = None
    invalid_00169 = None
    invalid_00480 = None
    invalid_00480_onc = None
    invalid_00381_00440_00410_00190 = None
    invalid_00170 = None
    invalid_00680 = None
    invalid_00161 = None
    invalid_stage = None

    while True:
        print("\nMenu:")
        print("1. Load data")
        print("2. Perform Step1B TNM edits (Clean data)")
        print("3. Generate final stage group edits")
        print("4. Generate Invalid SG for all schemas")
        print("5. Generate Invalid SG for all schemas 2")
        print("6. Generate Invalid Stage Grade (00381, 00440, 00410, 00190)")
        print("7. Generate Invalid Schema 00580")
        print("8. Generate Invalid Schema 00730")
        print("9. Generate Invalid Schema 00111")
        print("10. Generate Invalid Schema 00560")
        print("11. Generate Invalid Schema 00161")
        print("12. Generate Invalid Schema 00430")
        print("13. Generate Invalid Schema 00169")
        print("14. Generate Invalid Schema 00590")
        print("15. Generate Invalid Schema 00480")
        print("16. Generate Invalid Schema 00480 (Oncology)")
        print("17. Generate Invalid Schema 00381, 00440, 00410, 00190")
        print("18. Generate Invalid Schema 00170")
        print("19. Generate Invalid Schema 00680")
        print("20. Generate Invalid Stage (TNM Only)")
        print("21. Export to Excel")
        print("22. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            file_path = input("Enter the path to the CSV file: ")
            tnm.load_data(file_path)
        elif choice == '2':
            tnm.step1b_tnmedits()
        elif choice == '3':
            final_edits = tnm.final_stagegroup_edits()
            print("Final stage group edits generated.")
        elif choice == '4':
            invalid_sg_allschemas = tnm.invalid_sg_allschemas()
            print("Invalid SG for all schemas generated.")
            print(invalid_sg_allschemas.head())
        elif choice == '5':
            invalid_sg_allschemas2 = tnm.invalid_sg_allschemas2()
            print("Invalid SG for all schemas 2 generated.")
            print(invalid_sg_allschemas2.head())
        elif choice == '6':
            sg_grade_path = input("Enter the path to the Excel file with SG Grade data: ")
            sg_grade = tnm.load_sg_data(sg_grade_path, 'TNM_GRADE')
            invalid_stage_grade_df = tnm.invalid_stage_grade(sg_grade)
            print("Invalid Stage Grade (00381, 00440, 00410, 00190) generated.")
            print(invalid_stage_grade_df.head())
        elif choice == '7':
            sg_data_path = input("Enter the path to the Excel file with Schema 00580 data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00580')
            invalid_00580 = tnm.invalid_schema_00580(sg_data)
            print("Invalid Schema 00580 generated.")
            print(invalid_00580.head())
        elif choice == '8':
            sg_data_path = input("Enter the path to the Excel file with Schema 00730 data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00730')
            invalid_00730 = tnm.invalid_schema_00730(sg_data)
            print("Invalid Schema 00730 generated.")
            print(invalid_00730.head())
        elif choice == '9':
            sg_data_path = input("Enter the path to the Excel file with Schema 00111 data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00111')
            invalid_00111 = tnm.invalid_schema_00111(sg_data)
            print("Invalid Schema 00111 generated.")
            print(invalid_00111.head())
        elif choice == '10':
            sg_data_path = input("Enter the path to the Excel file with Schema 00560 data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00560')
            invalid_00560 = tnm.invalid_schema_00560(sg_data)
            print("Invalid Schema 00560 generated.")
            print(invalid_00560.head())
        elif choice == '11':
            sg_data_path = input("Enter the path to the Excel file with Schema 00161 data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00161')
            invalid_00161 = tnm.invalid_schema_00161(sg_data)
            print("Invalid Schema 00161 generated.")
            print(invalid_00161.head())
        elif choice == '12':
            sg_data_path = input("Enter the path to the Excel file with Schema 00430 data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00430')
            invalid_00430 = tnm.invalid_schema_00430(sg_data)
            print("Invalid Schema 00430 generated.")
            print(invalid_00430.head())
        elif choice == '13':
            sg_data_path = input("Enter the path to the Excel file with Schema 00169 data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00169')
            invalid_00169 = tnm.invalid_schema_00169(sg_data)
            print("Invalid Schema 00169 generated.")
            print(invalid_00169.head())
        elif choice == '14':
            sg_data_path = input("Enter the path to the Excel file with Schema 00590 data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00590')
            invalid_00590 = tnm.invalid_schema_00590(sg_data)
            print("Invalid Schema 00590 generated.")
            print(invalid_00590.head())
        elif choice == '15':
            sg_data_path = input("Enter the path to the Excel file with Schema 00480 data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00480')
            invalid_00480 = tnm.invalid_schema_00480(sg_data)
            print("Invalid Schema 00480 generated.")
            print(invalid_00480.head())
        elif choice == '16':
            sg_data_path = input("Enter the path to the Excel file with Schema 00480 (Oncology) data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00480_Oncology')
            invalid_00480_onc = tnm.invalid_schema_00480_onc(sg_data)
            print("Invalid Schema 00480 (Oncology) generated.")
            print(invalid_00480_onc.head())
        elif choice == '17':
            sg_grade_path = input("Enter the path to the Excel file with SG Grade data: ")
            sg_grade = tnm.load_sg_data(sg_grade_path, 'TNM_GRADE')
            invalid_00381_00440_00410_00190 = tnm.invalid_schema_00381_00440_00410_00190(sg_grade)
            print("Invalid Schema 00381, 00440, 00410, 00190 generated.")
            print(invalid_00381_00440_00410_00190.head())
        elif choice == '18':
            sg_data_path = input("Enter the path to the Excel file with Schema 00170 data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00170')
            invalid_00170 = tnm.invalid_schema_00170(sg_data)
            print("Invalid Schema 00170 generated.")
            print(invalid_00170.head())
        elif choice == '19':
            sg_data_path = input("Enter the path to the Excel file with Schema 00680 data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_00680')
            invalid_00680 = tnm.invalid_schema_00680(sg_data)
            print("Invalid Schema 00680 generated.")
            print(invalid_00680.head())
        elif choice == '20':
            sg_data_path = input("Enter the path to the Excel file with TNM Only data: ")
            sg_data = tnm.load_sg_data(sg_data_path, 'TNM_TNMonly')
            invalid_stage = tnm.invalid_stage(sg_data)
            print("Invalid Stage (TNM Only) generated.")
            print(invalid_stage.head())
        elif choice == '21':
            if final_edits is not None:
                file_path = input("Enter the path to save the Excel file: ")
                sheet_name = input("Enter the sheet name: ")
                tnm.export_to_excel(final_edits, file_path, sheet_name)
            else:
                print("Generate final stage group edits first.")
        elif choice == '22':
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()