# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 13:40:14 2024

@author: user
"""
import pandas as pd
import recordlinkage
<<<<<<< HEAD
from utility import Utility
import logging



class App:
    
    def __init__(self):
        
        # Initialize utility and configure Logging
        self._util = Utility()
        self._util.configureLogging
    
    def process(self):
        
        try:
            logging.info('****Start****')
                        
            
            df_table1 = pd.DataFrame({
                'acb_no': [201, 202, 203, 204, 205, 206, 140, 207, 208, 209, 210],
                'midnme': ['Smith', 'Johnson', 'Williams', 'Brown', 'Taylor', 'Clark', 'Chris', 'White', 'Green', 'Black', 'Grey'],
                'dob': ['1980-01-01', '1976-05-10', '1987-09-15', '1988-03-24', '1980-07-14', '1990-04-18', '2000-01-28', '1980-01-01', '1980-07-14', '1990-04-18', '1976-05-10'],
                'sex': ['M', 'F', 'M', 'F', 'M', 'F', 'M', 'M', 'F', 'M', 'F'],
                'lst_name': ['Smith', 'Johnson', 'Williams', 'Brown', 'Taylor', 'Clark', 'Blue', 'White', 'Green', 'Black', 'Grey'],
                'fst_name': ['John', 'Mary', 'Robert', 'Emily', 'Michael', 'Anna', 'Jorden', 'Tom', 'Jerry', 'Luke', 'Liam'],
                'snd_last': ['Doe', 'Martinez', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Lewis', 'Smith', 'Johnson', 'Williams', 'Brown'],
                'dth_dte': ['2020-03-15', '2018-12-20', '2021-08-05', '2019-11-10', '2022-01-01', '2023-07-11', '2023-01-28', '2020-03-15', '2018-12-20', '2021-08-05', '2019-11-10'],
                'uli': ['A123', 'B456', 'C789', '', 'D012', 'E345', 'F678', 'A123', 'B456', 'C789', 'D012'],
                'bsurn': ['X123', 'Y456', '', 'Z789', 'W012', 'U345', 'T678', 'X123', 'Y456', 'Z789', 'W012'],
                'ldgyr': ['2019-01-01', '2017-12-31', '2020-05-15', '2018-11-01', '2021-01-20', '2022-07-01', '2023-01-01', '2019-01-01', '2017-12-31', '2020-05-15', '2018-11-01'],
                'region': [1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4],
                'ACR_id_comment': ['comment924', 'other_comment', 'comment924', 'test_comment', 'another_comment', '924_comment', 'comment924', 'comment924', 'other_comment', 'comment924', 'test_comment'],
                'dob_stat': ['m', 'd', 'm', 'd', 'm', 'd', 'm', 'm', 'd', 'm', 'd']  # Example dob_stat values
            })
            
            df_table2 = pd.DataFrame({
                'acbno1': [101, 107, 103, 104, 105, 106, 140, 108, 109, 110, 111],
                'midnme1': ['Smith', 'Johnson', 'Williams', 'Brown', 'Taylor', 'Clark', 'Chris', 'White', 'Green', 'Black', 'Grey'],
                'dob1': ['1980-01-01', '1975-05-10', '1992-09-15', '1988-03-25', '1914-07-14', '1918-04-18', '1999-01-28', '1980-01-01', '1914-07-14', '1918-04-18', '1986-05-10'],
                'sex1': ['M', 'F', 'M', 'F', 'M', 'F', 'M', 'M', 'F', 'M', 'F'],
                'lstnme1': ['Smith', 'Johnson', 'Williams', 'Brown', 'Taylor', 'Clark', 'Blue', 'White', 'Green', 'Black', 'Grey'],
                'fstnme1': ['John', 'Mary', 'Robert', 'Emily', 'Michael', 'Anna', 'Jorden', 'Tom', 'Jerry', 'Luke', 'Liam'],
                'snd_last1': ['Doe', 'Martinez', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Lewis', 'Smith', 'Johnson', 'Williams', 'Brown'],
                'dth_dte1': ['2020-03-15', '2018-12-20', '2021-08-05', '2019-11-10', '2022-01-01', '2023-07-11', '2023-01-28', '2020-03-15', '2018-12-20', '2021-08-05', '2019-11-10'],
                'uli1': ['G123', 'B456', 'H789', 'I012', 'J345', 'K678', 'L901', 'G123', 'B456', 'H789', 'I012'],
                'bsurn1': ['P123', 'Q456', 'R789', 'S012', 'T345', 'U678', 'V901', 'P123', 'Q456', 'R789', 'S012'],
                'ldgyr1': ['2018-01-01', '2016-12-31', '2019-05-15', '2017-11-01', '2020-01-20', '2021-07-01', '2022-01-01', '2018-01-01', '2016-12-31', '2019-05-15', '2017-11-01'],
                'region1': [1, 2, 4, 4, 8, 1, 6, 1, 2, 3, 4],
                'ACR_id_comment1': ['no_comment', '924_test', 'sample_comment', 'comment924', 'test_924', 'comment_other', 'another_924', 'no_comment', '924_test', 'sample_comment', 'comment924'],
                'dob_stat': ['d', 'm', 'm', 'd', 'm', 'd', 'm', 'd', 'm', 'd', 'm']  # Example dob_stat values
            })
            
                        
            # Convert dictionaries to DataFrames
            maligCCR1 = pd.DataFrame( df_table1 )
            maligCCR2 = pd.DataFrame( df_table2 )
            
            # Convert date columns to datetime
            maligCCR1['dob'] = pd.to_datetime(maligCCR1 ['dob'])
            maligCCR2['dob1'] = pd.to_datetime(maligCCR2['dob1'])
            
            maligCCR1['dth_dte'] = pd.to_datetime(df_table1['dth_dte'])
            maligCCR2['dth_dte1'] = pd.to_datetime(df_table2['dth_dte1'])
            
            maligCCR1['ldgyr'] = pd.to_datetime(df_table1['ldgyr'])
            maligCCR2['ldgyr1'] = pd.to_datetime(df_table2['ldgyr1'])
                        
            Links1= self._util.create_link(maligCCR1, maligCCR2)
            
            logging.info('****Generating link****')

            
            print(Links1)
         
            
            Links1_step1 = self._util.add_file_number(Links1)
            
            print(Links1_step1)
            
            
            # Apply the function to each row
            Links1_step1['pflag'] = Links1_step1.apply(self._util.create_pflag, axis=1)
            print(Links1_step1)
            
            
            # Apply the function to assign regfinal
            Links1_step1['regfinal'] = Links1_step1.apply(self._util.assign_regfinal, axis=1)
        
            Links1_step1.sort_values(by=['fileno1', 'fileno2'], inplace=True) 
            Links1_step1.drop_duplicates(subset=['fileno1', 'fileno2'], keep = 'first', inplace = True )
            print(Links1_step1)
            
            archive_file = {
            'fileno1': ['0000101', '0000103', '0000105'],
            'fileno2': ['0000140', '0000203', '0000205'],
            'date_added': ['2020-01-01', '2020-01-02', '2020-01-03']
            }
            
            archive_data = pd.DataFrame(archive_file)
            
            # Convert date_added to datetime
            archive_data['date_added'] = pd.to_datetime(archive_data['date_added'])
            
            
            Links1_step2 = pd.merge(Links1_step1, archive_data, on=['fileno1', 'fileno2'], how='left', indicator=True)
            
            # Filter out records present in the archive (keep only 'left_only' rows)
            Links1_step2  = Links1_step2 [Links1_step2 ['_merge'] == 'left_only'].drop(columns=['_merge', 'date_added'])
            
            # Sorting the DataFrame
            Links1_step2 .sort_values(by=['fileno1', 'fileno2'], inplace=True)
            Links1_step2.sort_values(by=['regfinal', 'sex', 'lst_name', 'fstnme1'], inplace=True)

            
            print(Links1_step2)
            
            # Sanatize the arc comments
            Links1_step2 = Links1_step2.apply(self._util.sanitize_acr_comments, axis=1)
            Links1_step3 = self._util.filter_name_conditions(Links1_step2)
            print(Links1_step3)
            
            self._util.pflag_frequency_report(Links1_step3)
            
            # Generate Pflag report
            self._util.generate_pflag_report(Links1_step3, "pflag == 0", "AB DUPLICATES BOTH IN CCR YEARS")
            self._util.generate_pflag_report(Links1_step3, "pflag != 0", "REJECTION OF DUP BOTH IN CCR YEARS: DEATH DATES DIFFERENT OR TOO BROAD DOB OR BIRTHNAME OR PHN AND FIRST NAME DIFF")
            
            # Clean the 'ACR_id_comment' variable
            Links1_step2['ACR_id_comment'] = Links1_step2['ACR_id_comment'].str.replace('\n', ' ').str.replace('\r', ' ')
            Links1_step2['ACR_id_comment'] = Links1_step2['ACR_id_comment'].str.replace(',', '')
            
            
            conditions_pflag0 = self._util.get_conditions_pflag0(Links1_step2)
            conditions_pflag_not0 = self._util.get_conditions_pflag_not_0(Links1_step2)
            
            print(conditions_pflag0) 
            print(conditions_pflag_not0)
            
            self._util.write_to_file(conditions_pflag0, 'MATOOUT.txt')
            self._util.write_to_file(conditions_pflag_not0, 'RIDOOUT.txt')
            

          
        except Exception as e:
            logging.exception("Exception occured")



if __name__ == "__main__":
    app = App()
    app.process()













































=======
from recordlinkage.base import BaseCompareFeature
>>>>>>> 5cf710ccd8fc14484023a8383455371fb3b8d26d

# Create the dataframes
df_table1 = pd.DataFrame({
    'acb_no': [201, 202, 203, 204, 205, 206, 140],
    'midnme': ['Smith', 'Johnson', 'Williams', 'Brown', 'Taylor', 'Clark', 'Chris'],
    'dob': ['1980-01-01', '1976-05-10', '1987-09-15', '1988-03-24', '1980-07-14', '1990-04-18', '2000-01-28'],
    'sex': ['M', 'F', 'M', 'F', 'M', 'F', 'M'],
    'lst_name': ['Smith', 'Johnson', 'Williams', 'Brown', 'Taylor', 'Clark', 'Blue'],
    'fst_name': ['John', 'Mary', 'Robert', 'Emily', 'Michael', 'Anna', 'Jorden'],
    'snd_last': ['Doe', 'Martinez', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Lewis'],
    'dth_dte': ['2020-03-15', '2018-12-20', '2021-08-05', '2019-11-10', '2022-01-01', '2023-07-11', '2023-01-28'],
    'uli': ['A123', 'B456', 'C789', '', 'D012', 'E345', 'F678'],
    'bsurn': ['X123', 'Y456', '', 'Z789', 'W012', 'U345', 'T678'],
    'ldgyr': ['2019-01-01', '2017-12-31', '2020-05-15', '2018-11-01', '2021-01-20', '2022-07-01', '2023-01-01'],
    'region': [1, 2, 3, 4, 5, 6, 7],
    'ACR_id_comment': ['comment924', 'other_comment', 'comment924', 'test_comment', 'another_comment', '924_comment', 'comment924']
})

df_table2 = pd.DataFrame({
    'acbno1': [101, 107, 103, 104, 105, 106, 140],
    'midnme1': ['Smith', 'Johnson', 'Williams', 'Brown', 'Taylor', 'Clark', 'Chris'],
    'dob1': ['1980-01-01', '1975-05-10', '1992-09-15', '1988-03-25', '1985-07-14', '1990-04-18', '1999-01-28'],
    'sex1': ['M', 'F', 'M', 'F', 'M', 'F', 'M'],
    'lstnme1': ['Smith', 'Johnson', 'Williams', 'Brown', 'Taylor','Clark', 'Blue'],
    'fstnme1': ['John', 'Mary', 'Robert', 'Emily', 'Michael', 'Anna', 'Jorden'],
    'snd_last1': ['Doe', 'Martinez', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Lewis'],
    'dth_dte1': ['2020-03-15', '2018-12-20', '2021-08-05', '2019-11-10', '2022-01-01', '2023-07-11', '2023-01-28'],
    'uli1': ['G123', 'B456', 'H789', 'I012', 'J345', 'K678', 'L901'],
    'bsurn1': ['P123', 'Q456', 'R789', 'S012', 'T345', 'U678', 'V901'],
    'ldgyr1': ['2018-01-01', '2016-12-31', '2019-05-15', '2017-11-01', '2020-01-20', '2021-07-01', '2022-01-01'],
    'region1': [1, 2, 4, 4, 8, 1, 6],
    'ACR_id_comment1': ['no_comment', '924_test', 'sample_comment', 'comment924', 'test_924', 'comment_other', 'another_924']
})

# Convert dob columns to datetime before renaming
df_table1['dob'] = pd.to_datetime(df_table1['dob'])
df_table2['dob1'] = pd.to_datetime(df_table2['dob1'])

# Rename columns in df_table2 to match df_table1
df_table2 = df_table2.rename(columns={

    'dob1': 'dob',
    'sex1': 'sex',
    
})

# Custom comparison class for combined date logic and age difference
class CustomDateAndAgeComparison(BaseCompareFeature):

    def _compute_vectorized(self, dob1, dob2):
        condition1 = (dob1 == dob2)
        condition2 = (dob2.dt.year - 1900 == dob1.dt.day) & (dob2.dt.day == (dob1.dt.year - 1900)) & (dob2.dt.month == dob1.dt.month)
        condition3 = (dob2.dt.month == (dob1.dt.year - 1900)) & ((dob2.dt.year - 1900) == dob1.dt.month) & (dob2.dt.day == dob1.dt.day)
        condition4 = (dob2.dt.year == dob1.dt.year) & (dob2.dt.day == dob1.dt.month) & (dob2.dt.month == dob1.dt.day)
        condition5 = (abs((dob1 - dob2).dt.days / 365.25) <= 10)
        result = (condition1 | condition2 | condition3 | condition4) & condition5
        return result.astype(int)


# Custom comparison class for acb_no not equal
class CustomNotEqualComparison(BaseCompareFeature):

    def _compute_vectorized(self, s1, s2):
        return (s1 != s2).astype(int) 


# Custom comparison class for first name matching
class CustomFirstNameComparison(BaseCompareFeature):

    def _compute_vectorized(self, s1, s2_1, s2_2):
        return ((s1 == s2_1) | (s1 == s2_2)).astype(int)
    

# Initialize the indexer
indexer = recordlinkage.Index()
indexer.block('sex')

# Create candidate links
candidate_links = indexer.index(df_table1, df_table2)

# Initialize the comparison
compare_cl = recordlinkage.Compare()

# Add comparison rules
compare_cl.add(CustomNotEqualComparison('acb_no', 'acb_no'), label = 'acb_number')
compare_cl.exact('snd_last', 'snd_last', label = 'snd_name')
compare_cl.add(CustomDateAndAgeComparison('dob', 'dob'), label = 'last_name')
compare_cl.exact('lst_name', 'lst_name', label = 'last_name')
compare_cl.add(CustomFirstNameComparison('fst_name', 'fst_name', 'midnme'), label = 'first_name')
compare_cl.exact('dth_dte', 'dth_dte', label ='death_date')

# Compute comparison vectors
feat = CustomFeature()

comparison_vectors = compare_cl.compute(candidate_links, df_table1, df_table2)

# Print the comparison results with column names
comparison_results = comparison_vectors.reset_index()
comparison_results.columns = ['rec_id_1', 'rec_id_2', 'acb_no', 'snd_last', 'dob', 'lst_name', 'name', 'date_of_death']

# Filter matches based on all criteria being met (assuming a match if all criteria are 1)
matches = comparison_results[(comparison_results[['acb_no', 'snd_last', 'dob', 'lst_name', 'name', 'date_of_death']].sum(axis=1) == 6)]

# Get the indices of matches in df_table1 and df_table2
matches_indices_table1 = matches['rec_id_1'].values
matches_indices_table2 = matches['rec_id_2'].values

# Filter out duplicates from df_table1
non_dup = df_table1[~df_table1.index.isin(matches_indices_table1)]

# Display the non-duplicate rows
print(non_dup)
