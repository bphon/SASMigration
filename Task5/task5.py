import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import smtplib
from email.message import EmailMessage
import oracledb

class TNMEdits:
    def __init__(self):
        self.df = None
        self.engine = None

    def load_data(self, file_path):
        self.df = pd.read_csv(file_path)

    def load_sg_data(self, file_path, sheet_name):
        sg_data = pd.read_excel(file_path, sheet_name=sheet_name)
        sg_data = sg_data.drop_duplicates()
        return sg_data

    def step1b_tnmedits(self):
        self.df['path_t'] = self.df['ajcc8_path_t'].str[1:].str.strip()
        self.df['path_n'] = self.df['ajcc8_path_n'].str[1:].str.strip()
        self.df['path_m'] = self.df['ajcc8_path_m'].str[1:].str.strip()
        self.df['path_stage'] = self.df['ajcc8_path_stage'].str.strip()

        self.df['clin_t'] = self.df['ajcc8_clinical_t'].str[1:].str.strip()
        self.df['clin_n'] = self.df['ajcc8_clinical_n'].str[1:].str.strip()
        self.df['clin_m'] = self.df['ajcc8_clinical_m'].str[1:].str.strip()
        self.df['clin_stage'] = self.df['ajcc8_clinical_stage'].str.strip()

        self.df['post_t'] = self.df['ajcc8_postTherapy_t'].str[2:].str.strip()
        self.df['post_n'] = self.df['ajcc8_postTherapy_n'].str[2:].str.strip()
        self.df['post_m'] = self.df['ajcc8_postTherapy_m'].str[1:].str.strip()
        self.df['post_stage'] = self.df['ajcc8_postTherapy_stage'].str.strip()

        self.df['agegrp'] = self.df['age_diag'].apply(lambda x: "<55" if x < 55 else ">=55")

        self.df['PBLOODINVO'] = self.df['PERIPHERAL_BLOOD_INVO'].str.strip().replace('.', '')

        self.df['ajcc8_id_new'] = self.df['ajcc8_id']

        self.format_PSA()

    def format_PSA(self):
        self.df['PSA_f'] = self.df['PSA'].apply(lambda x: None if x in ['XXX.1', 'XXX.2', 'XXX.3', 'XXX.7', 'XXX.9'] else float(x))

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
        invalid_sg_allschemas = pd.read_sql_query(text(query), self.engine)
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

    def load_sg_data(self, file_path, sheet_name):
        sg_data = pd.read_excel(file_path, sheet_name=sheet_name)
        sg_data = sg_data.drop_duplicates()
        return sg_data

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
        # Assume `sg_grade` is loaded previously
        sg_grade = self.load_sg_data('path_to_file', 'TNM_GRADE')
        invalid_stage_grade_df = self.invalid_stage_grade(sg_grade)
        
        # Combine with other invalid data (dummy placeholders for other schemas)
        other_invalid_data = pd.DataFrame()  # Replace with actual data loading and processing logic

        final_stagegroup_edits = pd.concat([
            invalid_stage_grade_df,
            other_invalid_data  # Add other processed DataFrames here
        ])
        final_stagegroup_edits['tnm_edit_flag'] = "2000 Invalid Overall Stage Group for T/N/M Combo"
        return final_stagegroup_edits

    def export_to_excel(self, df, file_path, sheet_name):
        df.to_excel(file_path, sheet_name=sheet_name, index=False)

    def send_email(self, to, subject, body, attachments=None):
        msg = EmailMessage()
        msg['To'] = to
        msg['Subject'] = subject
        msg.set_content(body)

        if attachments:
            for attachment in attachments:
                with open(attachment, 'rb') as f:
                    file_data = f.read()
                    file_name = attachment.split('/')[-1]
                    msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

        with smtplib.SMTP('localhost') as s:
            s.send_message(msg)

    def notify_success_or_error(self, success_attachment, error_message):
        try:
            final_stagegroup_edits = self.final_stagegroup_edits()
            self.export_to_excel(final_stagegroup_edits, 'path_to_output_file.xlsx', 'TNM Overall Stage Edits')
            self.send_email("EMAIL_ADDRESS", "TNM Stage Group Edits - Data for Review", "Please find attached the Frequency regarding TNM Stage Group Edits for Cleanups.", [success_attachment])
        except Exception as e:
            self.send_email("EMAIL_ADDRESS", "TNM Stage Group Edits - Error with Data", f"There was an error: {str(e)}")

# Usage example:
tnm = TNMEdits()
tnm.load_data('SASMigration\Task5\sampledata.csv')
tnm.step1b_tnmedits()
final_edits = tnm.final_stagegroup_edits()
tnm.export_to_excel(final_edits, 'SASMigration\Task5.xlsx', 'TNM Overall Stage Edits')
tnm.notify_success_or_error('SASMigration\Task5.xlsx', 'There was an error processing the TNM Stage Group Edits.')
