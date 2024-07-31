import pandas as pd
import os

class TNMEdits:
    def __init__(self):
        self.df = None  # DataFrame to hold the data
        
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

    def invalid_stage_grade(self):
        invalid_stage_grade = self.df.query(
            "(descriptor in ['c', 'cp'] and ((t_value == 'T' and clin_t.str.contains('%')) or clin_t.str.contains(t_value)) and ((n_value == 'N' and clin_n.str.contains('%')) or clin_n.str.contains(n_value)) and ((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and ((grade == 'G' and ajcc8_clinical_grade.str.contains('%')) or ajcc8_clinical_grade.str.contains(grade)) and clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and ((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and ((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and ((grade == 'G' and ajcc8_path_grade.str.contains('%')) or ajcc8_path_grade.str.contains(grade)) and path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and ((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and ((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and ((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and ((grade == 'G' and ajcc8_posttherapy_grade.str.contains('%')) or ajcc8_posttherapy_grade.str.contains(grade)) and post_stage != stage_group)"
        )
        invalid_stage_grade['tnm_edit2000'] = 1
        return invalid_stage_grade

    def invalid_sg_allschemas(self):
        invalid_sg_allschemas = self.df.query(
            "(ajcc8_path_t == ' ' and ajcc8_path_n == ' ' and ajcc8_path_m == ' ' and ajcc8_path_stage not in ['99', '88', ' ']) or "
            "(ajcc8_clinical_t == ' ' and ajcc8_clinical_n == ' ' and ajcc8_clinical_m == ' ' and ajcc8_clinical_stage not in ['99', '88']) or "
            "(ajcc8_posttherapy_t == ' ' and ajcc8_posttherapy_n == ' ' and ajcc8_posttherapy_m == ' ' and ajcc8_posttherapy_stage not in ['99', '88', ' '])"
        )
        invalid_sg_allschemas['tnm_edit2000'] = 1
        return invalid_sg_allschemas

    def invalid_sg_allschemas2(self):
        invalid_sg_allschemas2 = self.df.query(
            "schema_id not in ['00730'] and "
            "((ajcc8_path_t == ' ' and ajcc8_path_n == ' ' and path_m == 'M0' and ajcc8_path_stage not in ['99', '88']) or "
            "(ajcc8_clinical_t == ' ' and ajcc8_clinical_n == ' ' and clin_m == 'M0' and ajcc8_clinical_stage not in ['99', '88']) or "
            "(ajcc8_posttherapy_t == ' ' and ajcc8_posttherapy_n == ' ' and post_m == 'M0' and ajcc8_posttherapy_stage not in ['99', '88', ' ']))"
        )
        invalid_sg_allschemas2['tnm_edit2000'] = 1
        return invalid_sg_allschemas2

    def invalid_schema_00580(self):
        invalid_00580 = self.df.query(
            "((descriptor in ['c', 'cp'] and "
            "((t_value == 'T' and ajcc8_clinical_t.str.contains('%')) or ajcc8_clinical_t.str.contains(t_value)) and "
            "((n_value == 'N' and clin_n.str.contains('%')) or clin_n.str.contains(n_value)) and "
            "((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_clinical_grade.str.contains('%')) or ajcc8_clinical_grade.str.contains(grade)) and "
            "((PSA_X == 'XXX.1' and PSA == PSA_X) or (PSA == 'Any' and PSA.str.contains('%')) or (low <= PSA_f <= high)) and "
            "clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and "
            "((t_value == 'T' and ajcc8_path_t.str.contains('%')) or ajcc8_path_t.str.contains(t_value)) and "
            "((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_path_grade.str.contains('%')) or ajcc8_path_grade.str.contains(grade)) and "
            "((PSA_X == 'XXX.1' and PSA == PSA_X) or (PSA == 'Any' and PSA.str.contains('%')) or (low <= PSA_f <= high)) and "
            "path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and "
            "((t_value == 'T' and ajcc8_posttherapy_t.str.contains('%')) or ajcc8_posttherapy_t.str.contains(t_value)) and "
            "((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and "
            "((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_posttherapy_grade.str.contains('%')) or ajcc8_posttherapy_grade.str.contains(grade)) and "
            "((PSA_X == 'XXX.1' and PSA == PSA_X) or (PSA == 'Any' and PSA.str.contains('%')) or (low <= PSA_f <= high)) and "
            "post_stage != stage_group))"
        )
        invalid_00580['tnm_edit2000'] = 1
        return invalid_00580

    def invalid_schema_00730(self):
        invalid_00730 = self.df.query(
            "((descriptor in ['c', 'cp'] and "
            "((t_value == 'T' and clin_t.str.contains('%')) or clin_t.str.contains(t_value)) and "
            "((n_value == 'N' and clin_n.str.contains('%')) or clin_n.str.contains(n_value)) and "
            "((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and "
            "ajcc8_id == ajccid and "
            "((agegroup == 'A' and agegrp.str.contains('%')) or agegroup == agegrp) and "
            "clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and "
            "((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and "
            "((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "ajcc8_id == ajccid and "
            "((agegroup == 'A' and agegrp.str.contains('%')) or agegroup == agegrp) and "
            "path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and "
            "((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and "
            "((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and "
            "((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and "
            "ajcc8_id == ajccid and "
            "((agegroup == 'A' and agegrp.str.contains('%')) or agegroup == agegrp) and "
            "post_stage != stage_group))"
        )
        invalid_00730['tnm_edit2000'] = 1
        return invalid_00730

    def invalid_schema_00111(self):
        invalid_00111 = self.df.query(
            "((descriptor in ['c', 'cp'] and "
            "((t_value == 'T' and clin_t.str.contains('%')) or clin_t.str.contains(t_value)) and "
            "((n_value == 'N' and clin_n.str.contains('%')) or clin_n.str.contains(n_value)) and "
            "((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and "
            "discriminator2.str.contains(discriminator2) and "
            "clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and "
            "((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and "
            "((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "discriminator2.str.contains(discriminator2) and "
            "path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and "
            "((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and "
            "((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and "
            "((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and "
            "discriminator2.str.contains(discriminator2) and "
            "post_stage != stage_group))"
        )
        invalid_00111['tnm_edit2000'] = 1
        return invalid_00111

    def invalid_schema_00560(self):
        invalid_00560 = self.df.query(
            "((descriptor in ['c', 'cp'] and "
            "((t_value == 'T' and clin_t.str.contains('%')) or clin_t.str.contains(t_value)) and "
            "((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and "
            "gestational_prog_index.str.contains(gtpi) and "
            "clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and "
            "((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "gestational_prog_index.str.contains(gtpi) and "
            "path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and "
            "((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and "
            "((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and "
            "gestational_prog_index.str.contains(gtpi) and "
            "post_stage != stage_group))"
        )
        invalid_00560['tnm_edit2000'] = 1
        return invalid_00560

    def invalid_schema_00430(self):
        invalid_00430 = self.df.query(
            "((descriptor in ['c', 'cp'] and "
            "((t_value == 'T' and clin_t.str.contains('%')) or clin_t.str.contains(t_value)) and "
            "((n_value == 'N' and clin_n.str.contains('%')) or clin_n.str.contains(n_value)) and "
            "((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_clinical_grade.str.contains('%')) or ajcc8_clinical_grade.str.contains(grade)) and "
            "ajcc8_id == ajccid and "
            "clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and "
            "((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and "
            "((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_path_grade.str.contains('%')) or ajcc8_path_grade.str.contains(grade)) and "
            "ajcc8_id == ajccid and "
            "path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and "
            "((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and "
            "((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and "
            "((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_posttherapy_grade.str.contains('%')) or ajcc8_posttherapy_grade.str.contains(grade)) and "
            "ajcc8_id == ajccid and "
            "post_stage != stage_group))"
        )
        invalid_00430['tnm_edit2000'] = 1
        return invalid_00430

    def invalid_schema_00169(self):
        invalid_00169 = self.df.query(
            "(descriptor in ['p', 'cp'] and "
            "((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and "
            "((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_path_grade.str.contains('%')) or ajcc8_path_grade.str.contains(grade)) and "
            "path_stage != stage_group) or "
            "(descriptor == 'y' and ajcc8_path_stage == ' ' and "
            "((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and "
            "((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and "
            "((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_posttherapy_grade.str.contains('%')) or ajcc8_posttherapy_grade.str.contains(grade)) and "
            "post_stage != stage_group)"
        )
        invalid_00169['tnm_edit2000'] = 1
        return invalid_00169

    def invalid_schema_00590(self):
        invalid_00590 = self.df.query(
            "((descriptor in ['c', 'cp'] and "
            "((t_value == 'T' and clin_t.str.contains('%')) or clin_t.str.contains(t_value)) and "
            "((n_value == 'N' and clin_n.str.contains('%')) or clin_n.str.contains(n_value)) and "
            "((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and "
            "((s_cat == 'S' and s_category_clin.str.contains('%')) or s_category_clin.str.contains(s_cat)) and "
            "clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and "
            "((t_value in ['T', 'pT'] and path_t.str.contains('%')) or path_t.str.contains(t_value)) and "
            "((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "((s_cat == 'S' and s_category_path.str.contains('%')) or s_category_path.str.contains(s_cat)) and "
            "path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and "
            "((t_value in ['T', 'pT'] and post_t.str.contains('%')) or post_t.str.contains(t_value)) and "
            "((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and "
            "((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and "
            "((s_cat == 'S' and s_category_path.str.contains('%')) or s_category_path.str.contains(s_cat)) and "
            "post_stage != stage_group))"
        )
        invalid_00590['tnm_edit2000'] = 1
        return invalid_00590

    def invalid_schema_00480(self):
        invalid_00480 = self.df.query(
            "((descriptor in ['c', 'cp'] and "
            "((t_value == 'T' and ajcc8_clinical_t.str.contains('%')) or ajcc8_clinical_t.str.contains(t_value)) and "
            "((n_value == 'N' and ajcc8_clinical_n.str.contains('%')) or ajcc8_clinical_n.str.contains(n_value)) and "
            "((m_value == 'M' and ajcc8_clinical_m.str.contains('%')) or ajcc8_clinical_m.str.contains(m_value)) and "
            "ER == ER_value and "
            "PR == PR_value and "
            "HER2_SUMMARY == HER2_SUM_Value and "
            "ajcc8_clinical_grade == grade and "
            "ajcc8_clinical_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and "
            "(ONCOTYPE_DX_SCORE >= '011' and ONCOTYPE_DX_SCORE not in ['XX4']) and "
            "((t_value == 'T' and ajcc8_path_t.str.contains('%')) or ajcc8_path_t.str.contains(t_value)) and "
            "((n_value == 'N' and ajcc8_path_n.str.contains('%')) or ajcc8_path_n.str.contains(n_value)) and "
            "((m_value == 'M' and ajcc8_path_m.str.contains('%')) or ajcc8_path_m.str.contains(m_value)) and "
            "ER == ER_value and "
            "PR == PR_value and "
            "HER2_SUMMARY == HER2_SUM_Value and "
            "ajcc8_path_grade == grade and "
            "ajcc8_path_stage != stage_group))"
        )
        invalid_00480['tnm_edit2000'] = 1
        return invalid_00480

    def invalid_schema_00480_onc(self):
        invalid_00480_onc = self.df.query(
            "(descriptor in ['p', 'cp'] and "
            "(ONCOTYPE_DX_SCORE < '011' or ONCOTYPE_DX_SCORE in ['XX4']) and "
            "((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and "
            "((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "ER == ER_value and "
            "HER2_SUMMARY == HER2_SUM_Value and "
            "path_stage != stage_group)"
        )
        invalid_00480_onc['tnm_edit2000'] = 1
        return invalid_00480_onc

    def invalid_schema_00381_00440_00410_00190(self):
        invalid_schemas = self.df.query(
            "((descriptor in ['c', 'cp'] and "
            "((t_value == 'T' and clin_t.str.contains('%')) or clin_t.str.contains(t_value)) and "
            "((n_value == 'N' and clin_n.str.contains('%')) or clin_n.str.contains(n_value)) and "
            "((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_clinical_grade.str.contains('%')) or ajcc8_clinical_grade.str.contains(grade)) and "
            "clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and "
            "((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and "
            "((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_path_grade.str.contains('%')) or ajcc8_path_grade.str.contains(grade)) and "
            "path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and "
            "((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and "
            "((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and "
            "((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_posttherapy_grade.str.contains('%')) or ajcc8_posttherapy_grade.str.contains(grade)) and "
            "post_stage != stage_group))"
        )
        invalid_schemas['tnm_edit2000'] = 1
        return invalid_schemas

    def invalid_schema_00170(self):
        invalid_00170 = self.df.query(
            "((descriptor in ['c', 'cp'] and "
            "((t_value == 'T' and clin_t.str.contains('%')) or clin_t.str.contains(t_value)) and "
            "((n_value == 'N' and clin_n.str.contains('%')) or clin_n.str.contains(n_value)) and "
            "((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and "
            "clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and "
            "((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and "
            "((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "path_stage != stage_group) or "
            "(descriptor == 'y' and ajcc8_path_stage == ' ' and "
            "((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and "
            "((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and "
            "((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and "
            "post_stage != stage_group)"
        )
        invalid_00170['tnm_edit2000'] = 1
        return invalid_00170

    def invalid_schema_00680(self):
        invalid_00680 = self.df.query(
            "((descriptor in ['c', 'cp'] and "
            "((t_value == 'T' and ajcc8_clinical_t.str.contains('%')) or ajcc8_clinical_t.str.contains(t_value)) and "
            "((n_value == 'N' and ajcc8_clinical_n.str.contains('%')) or ajcc8_clinical_n.str.contains(n_value)) and "
            "((m_value == 'M' and ajcc8_clinical_m.str.contains('%')) or ajcc8_clinical_m.str.contains(m_value)) and "
            "clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and "
            "((t_value == 'T' and ajcc8_path_t.str.contains('%')) or ajcc8_path_t.str.contains(t_value)) and "
            "((n_value == 'N' and ajcc8_path_n.str.contains('%')) or ajcc8_path_n.str.contains(n_value)) and "
            "((m_value == 'M' and ajcc8_path_m.str.contains('%')) or ajcc8_path_m.str.contains(m_value)) and "
            "path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and "
            "((t_value == 'T' and ajcc8_posttherapy_t.str.contains('%')) or ajcc8_posttherapy_t.str.contains(t_value)) and "
            "((n_value == 'N' and ajcc8_posttherapy_n.str.contains('%')) or ajcc8_posttherapy_n.str.contains(n_value)) and "
            "((m_value == 'M' and ajcc8_posttherapy_m.str.contains('%')) or ajcc8_posttherapy_m.str.contains(m_value)) and "
            "post_stage != stage_group))"
        )
        invalid_00680['tnm_edit2000'] = 1
        return invalid_00680

    def invalid_schema_00161(self):
        invalid_00161 = self.df.query(
            "(descriptor in ['p', 'cp'] and "
            "((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and "
            "((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_path_grade.str.contains('%')) or ajcc8_path_grade.str.contains(grade)) and "
            "((location == 'L' and ESOPHAGUS_EGJ_TUMOR_CENTER.str.contains('%')) or ESOPHAGUS_EGJ_TUMOR_CENTER.str.contains(location)) and "
            "path_stage != stage_group) or "
            "(descriptor == 'y' and ajcc8_path_stage == ' ' and "
            "((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and "
            "((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and "
            "((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and "
            "((grade == 'G' and ajcc8_posttherapy_grade.str.contains('%')) or ajcc8_posttherapy_grade.str.contains(grade)) and "
            "((location == 'L' and ESOPHAGUS_EGJ_TUMOR_CENTER.str.contains('%')) or ESOPHAGUS_EGJ_TUMOR_CENTER.str.contains(location)) and "
            "post_stage != stage_group)"
        )
        invalid_00161['tnm_edit2000'] = 1
        return invalid_00161

    def invalid_stage(self):
        invalid_stage = self.df.query(
            "((descriptor in ['c', 'cp'] and "
            "((t_value == 'T' and clin_t.str.contains('%')) or clin_t.str.contains(t_value)) and "
            "((n_value == 'N' and clin_n.str.contains('%')) or clin_n.str.contains(n_value)) and "
            "((m_value == 'M' and clin_m.str.contains('%')) or clin_m.str.contains(m_value)) and "
            "clin_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and "
            "((t_value == 'T' and path_t.str.contains('%')) or path_t.str.contains(t_value)) and "
            "((n_value == 'N' and path_n.str.contains('%')) or path_n.str.contains(n_value)) and "
            "((m_value == 'M' and path_m.str.contains('%')) or path_m.str.contains(m_value)) and "
            "path_stage != stage_group) or "
            "(descriptor in ['p', 'cp'] and ajcc8_path_stage == ' ' and "
            "((t_value == 'T' and post_t.str.contains('%')) or post_t.str.contains(t_value)) and "
            "((n_value == 'N' and post_n.str.contains('%')) or post_n.str.contains(n_value)) and "
            "((m_value == 'M' and post_m.str.contains('%')) or post_m.str.contains(m_value)) and "
            "post_stage != stage_group))"
        )
        invalid_stage['tnm_edit2000'] = 1
        return invalid_stage

    def final_stagegroup_edits(self, output_file_path):
        # Generate invalid stage grade data
        invalid_stage_grade_df = self.invalid_stage_grade()
        
        # Generate other invalid data
        invalid_00580 = self.invalid_schema_00580()
        invalid_00730 = self.invalid_schema_00730()
        invalid_00111 = self.invalid_schema_00111()
        invalid_00560 = self.invalid_schema_00560()
        invalid_00430 = self.invalid_schema_00430()
        invalid_00169 = self.invalid_schema_00169()
        invalid_00480 = self.invalid_schema_00480()
        invalid_00480_onc = self.invalid_schema_00480_onc()
        invalid_00381_00440_00410_00190 = self.invalid_schema_00381_00440_00410_00190()
        invalid_00170 = self.invalid_schema_00170()
        invalid_00680 = self.invalid_schema_00680()
        invalid_00161 = self.invalid_schema_00161()
        invalid_stage = self.invalid_stage()
        
        # Combine all data into one DataFrame
        combined_data = pd.concat([
            invalid_stage_grade_df,
            invalid_00580,
            invalid_00730,
            invalid_00111,
            invalid_00560,
            invalid_00430,
            invalid_00169,
            invalid_00480,
            invalid_00480_onc,
            invalid_00381_00440_00410_00190,
            invalid_00170,
            invalid_00680,
            invalid_00161,
            invalid_stage
        ], ignore_index=True)
        
        combined_data['tnm_edit_flag'] = "2000 Invalid Overall Stage Group for T/N/M Combo"
        
        # Export the combined DataFrame to a new Excel file
        self.export_to_excel(combined_data, output_file_path, 'Final_StageGroup_Edits')
        
        return combined_data

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
        print("3. Generate final stage group edits (combine all data)")
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
            output_file_path = input("Enter the output file path: ")
            final_edits = tnm.final_stagegroup_edits(output_file_path)
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
            invalid_stage_grade_df = tnm.invalid_stage_grade()
            print("Invalid Stage Grade (00381, 00440, 00410, 00190) generated.")
            print(invalid_stage_grade_df.head())
        elif choice == '7':
            invalid_00580 = tnm.invalid_schema_00580()
            print("Invalid Schema 00580 generated.")
            print(invalid_00580.head())
        elif choice == '8':
            invalid_00730 = tnm.invalid_schema_00730()
            print("Invalid Schema 00730 generated.")
            print(invalid_00730.head())
        elif choice == '9':
            invalid_00111 = tnm.invalid_schema_00111()
            print("Invalid Schema 00111 generated.")
            print(invalid_00111.head())
        elif choice == '10':
            invalid_00560 = tnm.invalid_schema_00560()
            print("Invalid Schema 00560 generated.")
            print(invalid_00560.head())
        elif choice == '11':
            invalid_00161 = tnm.invalid_schema_00161()
            print("Invalid Schema 00161 generated.")
            print(invalid_00161.head())
        elif choice == '12':
            invalid_00430 = tnm.invalid_schema_00430()
            print("Invalid Schema 00430 generated.")
            print(invalid_00430.head())
        elif choice == '13':
            invalid_00169 = tnm.invalid_schema_00169()
            print("Invalid Schema 00169 generated.")
            print(invalid_00169.head())
        elif choice == '14':
            invalid_00590 = tnm.invalid_schema_00590()
            print("Invalid Schema 00590 generated.")
            print(invalid_00590.head())
        elif choice == '15':
            invalid_00480 = tnm.invalid_schema_00480()
            print("Invalid Schema 00480 generated.")
            print(invalid_00480.head())
        elif choice == '16':
            invalid_00480_onc = tnm.invalid_schema_00480_onc()
            print("Invalid Schema 00480 (Oncology) generated.")
            print(invalid_00480_onc.head())
        elif choice == '17':
            invalid_00381_00440_00410_00190 = tnm.invalid_schema_00381_00440_00410_00190()
            print("Invalid Schema 00381, 00440, 00410, 00190 generated.")
            print(invalid_00381_00440_00410_00190.head())
        elif choice == '18':
            invalid_00170 = tnm.invalid_schema_00170()
            print("Invalid Schema 00170 generated.")
            print(invalid_00170.head())
        elif choice == '19':
            invalid_00680 = tnm.invalid_schema_00680()
            print("Invalid Schema 00680 generated.")
            print(invalid_00680.head())
        elif choice == '20':
            invalid_stage = tnm.invalid_stage()
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
