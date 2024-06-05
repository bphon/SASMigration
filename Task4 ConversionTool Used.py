# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 14:26:04 2024

@author: brianphong
"""

import pandas as pd

# Set options
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Define formats
NFMT = {1: 'current', 2: 'usual', 3: 'preferred', 4: 'birth', 5: 'other', 6: 'Not defined'}

# Load data
registry = pd.read_csv("ABCBM_2024_2024_04_29.csv")
regsupp = pd.read_csv("ABCBM_2024_2024_04_29.csv")

# Define macros
rdate = "2024_05_23"
lyear = 2018
sndcnt = 13
condsel = "(vit_stat != 'd' or year_death > lyear)"

# Step 1: Get all active cases
person = pd.concat([registry['nrperson'], registry['person']], ignore_index=True)
person = person.fillna({'num_mal': 0})

censor = regsupp[['acb_no', 'last_known_code', 'last_known_date']].drop_duplicates()

person = person.drop_duplicates('acb_no')

step1 = pd.merge(person, regsupp['spouse'], how='left', on='acb_no')
step1 = pd.merge(step1, registry['locator'], how='left', on='acb_no')
step1 = pd.merge(step1, censor, how='left', on='acb_no')

step1['actv_dte'] = step1['actv_dte'].fillna(pd.Timestamp('1900-01-01'))
step1['ACRactvyr'] = step1['actv_dte'].dt.year
step1 = step1.query(condsel)

step1['ACRbirth_surn'] = step1['birth_surn'].str.upper()
step1['ACRbth_plc'] = step1['bth_plc'].str.upper()
step1['ACRspouse'] = step1['spouse'].str.upper()
step1['ACRcur_town'] = step1['city'].str.upper()
step1['ACRcur_prov'] = step1['province'].str.upper()
step1['ACRcur_pc'] = step1['p_code'].str.upper()
step1['ACRcur_str'] = step1['st_addr'].str.upper()

step1 = step1.rename(columns={'full_name': 'ACRfull_name', 'dcno': 'ACRdcno', 'sex': 'ACRsex', 'dob': 'ACRdob',
                              'dob_stat': 'ACRdob_stat', 'vsdcno': 'ACRvsdcno', 'vshosp': 'ACRvshosp',
                              'vsicdu': 'ACRvsicdu', 'ms': 'ACRms', 'vsres_postcd': 'ACRvsres_postcd',
                              'vsres_town': 'ACRvsres_town', 'conf_src': 'ACRdth_conf_src', 'dth_dte': 'ACRdth_dte',
                              'dth_stat': 'ACRdth_stat', 'dth_loc': 'ACRdth_loc'})

step1 = step1.drop(['actv_dte', 'coder_dth_cs1_icd9', 'coder_dth_cs2_icd9', 'bth_plc', 'spouse', 'city', 'province',
                    'p_code', 'st_addr'], axis=1)

step1 = step1[['acb_no', 'ACRfull_name', 'ACRbirth_surn', 'vit_stat', 'num_mal', 'ACRsex', 'ACRdcno', 'uliN',
                'uli_status', 'ACRdob', 'ACRdob_stat', 'year_death', 'ACRdth_dte', 'ACRdth_stat', 'ACRdth_conf_src',
                'ACRdth_loc', 'coder_dth_cs_icd10', 'ACRbth_plc', 'ACRspouse', 'ACRcur_town', 'ACRcur_pc',
                'ACRcur_prov', 'ACRcur_str', 'ACRactvyr', 'ACRms', 'last_known_code', 'last_known_date',
                'last_appointment_dte', 'last_diag_dte', 'f_loc', 'medium_best', 'region', 'person_id', 'media_all',
                'ACRvsdcno', 'ACRvshosp', 'ACRvsicdu', 'ACRvsres_postcd', 'ACRvsres_town', 'ACR_id_comment']]

step1 = step1.drop_duplicates('acb_no')

# Step 2: Process alternate names
allnames = regsupp['alt_name'].copy()
allnames['id_name'] = allnames.groupby('acb_no').cumcount() + 1
allnames['ACRname_type'] = allnames['name_typ']
allnames['ACRsex'] = allnames['sex'].str.upper()
allnames['ACRsurname'] = allnames['lst_name'].str.upper()
allnames['ACRgiven_1st'] = allnames['fst_name'].str.upper()
allnames['ACRgiven_2nd'] = allnames['mid_name'].str.split().str[0].str.upper()
allnames['ACRgiven_3rd'] = allnames['mid_name'].str.split().str[1].str.upper()
allnames['ACRinit_1st'] = allnames['ACRgiven_1st'].str[0]
allnames['ACRinit_2nd'] = allnames['ACRgiven_2nd'].str[0]
allnames = allnames.drop(['sex', 'lst_name', 'fst_name', 'mid_name', 'name_typ', 'ncode'], axis=1)

step2 = pd.merge(step1, allnames, how='left', on='acb_no')
step2 = step2.sort_values(['acb_no', 'nflag', 'id_name'])
step2 = step2.fillna({'ACRsurname': ''})

step2['nflag'] = step2['nflag'].map(NFMT)
step2 = step2.rename(columns={'nflag': 'hierarchical code for name type eg 1=current'})
step2 = step2.assign(num_mal=lambda x: x['num_mal'].astype(int).astype(str).str.zfill(2),
                     id_name=lambda x: x['id_name'].astype(int).astype(str).str.zfill(2),
                     ACRvsdcno=lambda x: x['ACRvsdcno'].astype(int).astype(str).str.zfill(6),
                     ACRdcno=lambda x: x['ACRdcno'].astype(int).astype(str).str.zfill(6))

CRfile = step2.copy()


