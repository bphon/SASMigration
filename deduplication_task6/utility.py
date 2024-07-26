# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 10:55:35 2024

@author: jeromecorpuz
"""

import os
import logging 
from datetime import datetime 
import pandas as pd
import recordlinkage 

class Utility:
    
    def configureLogging(self):
        path =  os.path.join(os.getcw(), 'logs')
        if not os.path.ecists(path):
            os.makedirs(path)
        path = os.path.join(path, 'logfile_{:%Y%m%d}.txt'.format(datetime.now()))
        logging.basicConfig(filename=path,
        level = logging.DEBUG, format='%(asctime)s %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(ch) 
        
        
    # Add matflag to link1b
    def assign_matflag(self, row):
        if (abs(row['dob'].year - row['dob1'].year) + abs(row['dob'].month - row['dob1'].month) + abs(row['dob'].day - row['dob1'].day)) == 1:
            return 1
        elif (row['dob'].month == row['dob1'].month and row['dob'].day == row['dob1'].day) or \
             (row['dob'].year == row['dob1'].year and row['dob'].day == row['dob1'].day) or \
             (row['dob'].year == row['dob1'].year and row['dob'].month == row['dob1'].month):
            return 2
        elif (row['dob_stat'] == 'm' and abs(row['dob'].year - row['dob1'].year) == 1) or \
             (row['dob_stat'] == 'd' and (abs(row['dob'].year - row['dob1'].year) + abs(row['dob'].month - row['dob1'].month) == 1)):
            return 3
        else:
            return 9
        
    #apply file1 and file2 columns
    def add_file_number (self, Link): 
        # Initialize fileno1 and fileno2 columns
        fileno1 = []
        fileno2 = []
        
                
        # Determine fileno1 and fileno2 based on acb_no and acbno
        for index, row in Link.iterrows():
            if row['acb_no'] > row['acbno1']:
                fileno1.append(str(row['acbno1']).zfill(7))
                fileno2.append(str(row['acb_no']).zfill(7))
            else:
                fileno1.append(str(row['acb_no']).zfill(7))
                fileno2.append(str(row['acbno1']).zfill(7))
        
        # Add fileno1 and fileno2 to the DataFrame
        Link['fileno1'] = fileno1
        Link['fileno2'] = fileno2
        
        return Link

        
    
    # Define a function to check the conditions
    def create_pflag(self, row):
        # Initial conditions for method one
        condition1 = row['dob'] == row['dob1']
        condition2 = (row['dob1'].year - 1900 == row['dob'].day) and (row['dob1'].day == row['dob'].year - 1900) and (row['dob1'].month == row['dob'].month)
        condition3 = (row['dob1'].month == row['dob'].year - 1900) and (row['dob1'].year - 1900 == row['dob'].month) and (row['dob1'].day == row['dob'].day)
        condition4 = (row['dob1'].year == row['dob'].year) and (row['dob1'].day == row['dob'].month) and (row['dob1'].month == row['dob'].day)
        condition5 = row['uli'] and row['uli1'] and row['uli'] != row['uli1'] and row['fst_name'] != row['fstnme1']
        condition6 = row['bsurn'] and row['bsurn1'] and row['bsurn'] != row['bsurn1']
    
        # Additional conditions for pflag = 2
        condition7 = row['uli'] and row['uli1'] and row['uli'] != row['uli1']
        condition8 = row['bsurn'] and row['bsurn1'] and row['bsurn'] != row['bsurn']
    
         # Additional conditions for pflag = 3
        condition9 = abs((row['dth_dte'] - row['dth_dte1']).days) > 60
        condition10 = row['ldgyr'] > row['dth_dte1']
        condition11 = row['ldgyr1'] > row['dth_dte']
    
        if condition1 or condition2 or condition3 or condition4 or condition5 or condition6:
            return 1
        elif condition7 or condition8:
            return 2
        elif condition9 or condition10 or condition11:
            return 3 
        else:
            return 0  
        
    # Define a function to assign regfinal
    def assign_regfinal(self, row):
        if row['region'] == row['region1']:
            return row['region']
        elif row['region'] == 4 or row['region1'] == 4 or row['region'] == 8 or row['region1'] == 8:
            return 8
        elif row['region'] == 1 or row['region1'] == 1:
            return 9
        else:
            return 7 
    
    
    # Define the function to apply conditions
    def sanitize_acr_comments(self, row):
        if row['acb_no'] in ['e285765', 'g006715', 'n410255']:
            row['ACR_id_comment'] = '  '
    
        if '924' in row['ACR_id_comment']:
            row['ACR_id_comment'] = '  '
        
        if '924' in row['ACR_id_comment1']:
            row['ACR_id_comment1'] = '  '
            
        return row
    
    def filter_name_conditions(self, Link):
       
        
        # Add the increment nn column
        Link['nn'] = range(1, len(Link) + 1) 
        
        # Condition 1: Check if the first character of fstnme1 is equal to the first character of fst_name
        condition1 = Link['fstnme1'].str[0] == Link['fst_name'].str[0]
        
        # Condition 2: Check if mid_name is not equal to two spaces ("  ") and the first character of fstnme1 is equal to the first character of mid_name
        condition2 = (Link['midnme'].str.strip() != "") & (Link['fstnme1'].str[0] == Link['midnme'].str[0])
        
        # Condition 3: Check if midnme1 is not equal to two spaces ("  ") and the first character of fst_name is equal to the first character of midnme1
        condition3 = (Link['midnme1'].str.strip() != "") & (Link['fst_name'].str[0] == Link['midnme1'].str[0])
        
        # Combine the conditions using logical OR
        result = condition1 | condition2 | condition3
        
        return Link[result]

    
    def pflag_frequency_report(self, Link):
        
        # Frequency analysis (equivalent to PROC FREQ)
        frequency_table = Link.groupby(['regfinal', 'pflag']).size().reset_index(name='counts')
        
        # Pivot the table to get the desired format
        frequency_pivot = frequency_table.pivot(index='regfinal', columns='pflag', values='counts').fillna(0).astype(int)
        frequency_pivot.columns = [f'pflag={col}' for col in frequency_pivot.columns]
        
        # Reset index to bring 'regfinal' back as a column
        frequency_pivot.reset_index(inplace=True)
        
        return print(frequency_pivot)
    
    # Function to print data based on conditions
    def generate_pflag_report(self, df, condition, title):
        
        filtered_df = df.query(condition)
        print(f"\n{title}\n")
        print(filtered_df)
        
    
    def get_conditions_pflag0(self, df):
        # Conditions for pflag == 0
        conditions_pflag0 = (
            (df['pflag'] == 0) &
            (df['fstnme1'] == df['fst_name']) |
            (df['midnme'].notna() & (df['fstnme1'] == df['midnme'])) |
            (df['midnme1'].notna() & (df['fst_name'] == df['midnme1'])) 
        
        )
        
        selected_observations0 = df[conditions_pflag0]
        
        return selected_observations0 
    
    def get_conditions_pflag_not_0(self, df):
       # Conditions for pflag != 0
       conditions_pflag_not0 = (
            (df['pflag'] != 0) &
            (df['fstnme1'].str[0] == df['fst_name'].str[0]) |
            (df['midnme'].notna() & (df['fstnme1'].str[0] == df['midnme'].str[0])) |
            (df['midnme1'].notna() & (df['fst_name'].str[0] == df['midnme1'].str[0])) 
        )

        
       selected_observations_not0 = df[conditions_pflag_not0]
        
       return selected_observations_not0 
   
    def write_to_file(self, df, file_name):
        with open(file_name, 'w') as file:
            for index, row in df.iterrows():
                file.write(f"{row['fileno1']:<10}{row['fileno2']:<10}{row['pflag']:<3}{row['ACR_id_comment']:<60}{pd.Timestamp.now().date()}\n")
        print(f"Data written to {file_name} successfully.")
    
    
    def create_link (self, df_table1, df_table2):
               
            indexer = recordlinkage.Index()
            indexer.block(left_on='sex', right_on='sex1', not_equal_on=('acb_no', 'acbno1'))
            
            
            # Create candidate links
            candidate_links = indexer.index(df_table1, df_table2)
            
            
            comp_cl = recordlinkage.Compare()
            
            comp_cl.exact('snd_last', 'snd_last1', label = 'snd_last')
            comp_cl.dateSwap('dob', 'dob1', label = 'date_of_birth')
            
            ''' 
             comp_cl.date('dob', 'dob1', label = 'date_of_birth') can be used
             instead to meaure extacts as 1, month-day swaps as 0.5 ensure
             link1a = features[features.sum(axis=1) >= 1.5 ] to account for swaps

             
            '''
            
            
            features = comp_cl.compute(candidate_links, df_table1, df_table2) 
            
            
            link1a = features[features.sum(axis=1) >= 2 ] 
            
            
            # Reset index to use record identifiers
            link1a = link1a.reset_index()
            
            # Extract indices for joining
            index_df_table1 = link1a['level_0']
            index_df_table2 = link1a['level_1']
            
            # Join the dataframes using the indices
            joined_df_table1 = df_table1.loc[index_df_table1].reset_index(drop=True)
            joined_df_table2 = df_table2.loc[index_df_table2].reset_index(drop=True)
            
            # Combine the joined dataframes
            link1 = pd.concat([joined_df_table1, joined_df_table2], axis=1)
            
            # Remove duplicates based on the combined DataFrame
            link1a = link1.drop_duplicates()
            
            print(link1a)
            
            comp_cl2 = recordlinkage.Compare()
            comp_cl2.exact('lst_name', 'lstnme1', label='last_name')
            comp_cl2.exact('fst_name', 'fstnme1', label = 'first_name') 
            comp_cl2.exact('fst_name', 'midnme1', label = 'first_name_equals_midnme1')
            comp_cl2.exact('dth_dte', 'dth_dte1', label = 'date_of_death')
            comp_cl2.date_proximity('dob', 'dob1', label = 'dob_proximity_within_10_years')
            
            features2 = comp_cl2.compute(candidate_links, df_table1, df_table2) 
            
            
            # Apply the conditions to filter link1b
            link1b = features2[(features2['last_name'] == 1) & 
                              (features2['date_of_death'] == 1) &
                              (features2['dob_proximity_within_10_years'] == 1) &
                              ((features2['first_name'] == 1) | (features2['first_name_equals_midnme1'] == 1))]
            
            
            
            # Reset index to use record identifiers
            link1b = link1b.reset_index()
            
            # Extract indices for joining
            index_df_table1 = link1b['level_0']
            index_df_table2 = link1b['level_1']
            
            # Join the dataframes using the indices
            joined_df_table1 = df_table1.loc[index_df_table1].reset_index(drop=True)
            joined_df_table2 = df_table2.loc[index_df_table2].reset_index(drop=True)
            
            # Combine the joined dataframes
            link1 = pd.concat([joined_df_table1, joined_df_table2], axis=1)
            
            # Remove duplicates based on the combined DataFrame
            link1b = link1.drop_duplicates()
            
                        
            # Combining the Links (Links1)
            Link= pd.concat([link1a, link1b], ignore_index=True)
            
            # Sorting and Deduplication
            Link.sort_values(by=['acb_no', 'acbno1'], inplace=True)
           
            # Drop rows where matflag is 'Exact Match'
            Link.drop_duplicates(subset=['acb_no', 'acbno1'], keep='first', inplace=True)
            
            return Link


    
    
    