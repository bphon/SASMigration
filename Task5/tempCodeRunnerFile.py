def invalid_stage_grade(self, sg_grade):
        merged_df = self.df.merge(sg_grade, how='inner', left_on='schema_id', right_on='schemaid')
        conditions = (
            ((merged_df['descriptor'].isin(['c', 'cp'])) &
             ((merged_df['t_value'] == 'T') | (merged_df['clin_t'].str.contains(merged_df['t_value']))) &
             ((merged_df['n_value'] == 'N') | (merged_df['clin_n'].str.contains(merged_df['n_value']))) &
             ((merged_df['m_value'] == 'M') | (merged_df['clin_m'].str.contains(merged_df['m_value']))) &
             ((merged_df['grade'] == 'G') | (merged_df['ajcc8_clinical_grade'].str.contains(merged_df['grade']))) &
             (merged_df['clin_stage'] != merged_df['stage_group'])) |
            ((merged_df['descriptor'].isin(['p', 'cp'])) &
             ((merged_df['t_value'] == 'T') | (merged_df['path_t'].str.contains(merged_df['t_value']))) &
             ((merged_df['n_value'] == 'N') | (merged_df['path_n'].str.contains(merged_df['n_value']))) &
             ((merged_df['m_value'] == 'M') | (merged_df['path_m'].str.contains(merged_df['m_value']))) &
             ((merged_df['grade'] == 'G') | (merged_df['ajcc8_path_grade'].str.contains(merged_df['grade']))) &
             (merged_df['path_stage'] != merged_df['stage_group'])) |
            ((merged_df['descriptor'].isin(['p', 'cp'])) &
             (merged_df['ajcc8_path_stage'] == ' ') &
             ((merged_df['t_value'] == 'T') | (merged_df['post_t'].str.contains(merged_df['t_value']))) &
             ((merged_df['n_value'] == 'N') | (merged_df['post_n'].str.contains(merged_df['n_value']))) &
             ((merged_df['m_value'] == 'M') | (merged_df['post_m'].str.contains(merged_df['m_value']))) &
             ((merged_df['grade'] == 'G') | (merged_df['ajcc8_posttherapy_grade'].str.contains(merged_df['grade']))) &
             (merged_df['post_stage'] != merged_df['stage_group']))
        )
        invalid_stage_grade = merged_df[conditions]
        invalid_stage_grade['tnm_edit2000'] = 1
        return invalid_stage_grade