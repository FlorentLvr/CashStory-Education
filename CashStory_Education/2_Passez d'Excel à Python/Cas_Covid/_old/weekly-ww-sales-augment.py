import pandas as pd  # noqa: F401
import numpy as np
import datetime
import re
import os
import glob
import requests

hc_key = '991b06de-e729-4cf8-8e00-a82ca29686f4'
# hc_key = '6b6e875a-549b-40a3-9d25-95ae0a00210e'


def hc_start(healthkey):
    requests.get('https://health.cashstory.com/ping/' +
                 healthkey + '/start')
    return healthkey + ' is started'


def hc_done(healthkey):
    requests.get('https://health.cashstory.com/ping/' + healthkey)
    return healthkey + ' is done'

# ----------------------------------------------------------------------------------------------------------------
# SCRIPT TRANSFORMATION DATASOURCES
# ----------------------------------------------------------------------------------------------------------------

# Drop columns in dataframe
def drop_cols(df, cols_to_drop):
    cols = df.columns

    for col in cols_to_drop: 
        if col in cols:
            df = df.drop(col, axis=1)
    return df

# Get last ref updated : 
def get_store(df_ref):
    # get last ref updated : date extract = max
    df_ref['FILENAME'] = df_ref['__filename__']
    df_ref['DATE'] = df_ref['__filename__'].str[11:-8]
    df_ref['SORT'] = pd.to_datetime(
        df_ref['DATE'], format='%Y-%m-%d %H.%M.%S').dt.strftime('%Y%m%d%H%M%S')
    date_max = df_ref['SORT'].max()
    df_ref = df_ref[df_ref['SORT'] == date_max]

    # cleaning dataframe
    to_drop = ['ZONE', 'REGION_N1', 'COUNTRY_N1',
               'LOCATION_TYPE_N0', 'LOCATION_TYPE_N1', 'LOCATION_TYPE_N2',
               'FILENAME', 'SORT', '__filename__', 'DATE']
    df_ref = df_ref.drop(to_drop, axis=1)
    return df_ref


# -- STEP1 : Filter data and select right file
def step1(df, db_ref):
    hc_start(hc_key)
    # remove columns not used
    to_drop = ['ENTITY', 'TYPOLOGY', 'COUNTRY', 'REGION',
               'LABEL_JDA', 'DEP_LABEL', 'CALENDAR_TYPE']
    df = df.drop(to_drop, axis=1)

    # merge with DB_STORE refentiel
    db_init = pd.merge(df, db_ref, on='CODE_JDA', how='left')

    # filter to remove "not defined" 
    db_init['LABEL_JDA'] =  db_init['LABEL_JDA'].fillna('Not Defined')

    # filter to select "certified" from tuesday to saturday and "merch" on sunday (7) and monday (1)
    db_init['FILENAME'] = db_init['__filename__']
    db_init['FILETYPE'] = db_init['FILENAME'].str[0:8]
    db_init['SORT'] = pd.to_datetime(
        db_init['FILENAME'].str[-27:-8], format='%Y-%m-%d %H.%M.%S').dt.strftime('%Y%m%d%H%M%S')
    db_init['DAY'] = pd.to_datetime(
        db_init['FILENAME'].str[-27:-8], format='%Y-%m-%d %H.%M.%S').dt.strftime('%u')
    db_init['MONTH'] = pd.to_datetime(
        db_init['WEEK'].str[6:-5], format='%B').dt.strftime('%m').astype(int)
    db_init['YEAR'] = db_init['WEEK'].str[-4:]

    # --- Get current week
    # filter files by days (if day == Monday (1) or day == Sunday (7) then select DB_ALL_M)
    df_current = db_init.copy()
    day = pd.to_numeric(datetime.datetime.now().strftime('%u'))
    if day == 1 or day == 7:
        df_current = df_current[df_current['FILETYPE'] == 'DB_ALL_M']
    else:
        df_current = df_current[df_current['FILETYPE'] == 'DB_ALL_C']

    # get last file updated : date extract = max
    current_date_max = df_current['SORT'].max()
    df_current = df_current[df_current['SORT'] == current_date_max]

    # --- Get last week / every last month week / last year week
    # 0-- Create df without current week and with only certified sales
    current_week = df_current.iloc[0]['WEEK']
    df = db_init[(db_init['FILETYPE'] == 'DB_ALL_C')
                 & (db_init['WEEK'] != current_week)]

    # 0-- Create df_last with last updated file for each week
    df_last = pd.DataFrame()
    weeks = df['WEEK'].drop_duplicates()
    for week in weeks:
        # - select only the week
        tmp_df = df.loc[df['WEEK'] == week]
        # - keep the max of each week
        date_max = tmp_df['SORT'].max()
        tmp_df = tmp_df[tmp_df['SORT'] == date_max]
        df_last = df_last.append(tmp_df, sort=False, ignore_index=True)

    # 1-- Get last week in df_last
    last_week = df_last['SORT'].max()
    df_last_week = df_last[df_last['SORT'] == last_week]

    # 2-- Get last week of every month in df_last
    # find max month in current year
    df_last_months = pd.DataFrame()
    current_year = str(int(current_date_max[:4]))
    x = df_last.loc[df_last['YEAR'] == current_year]['MONTH'].max()
    for i in range(1, x):
        # select month
        tmp_df = df_last.loc[df_last['MONTH'] == i]
        # keep the max of each month
        date_max = tmp_df['SORT'].max()
        if date_max != last_week:
            tmp_df = tmp_df[tmp_df['SORT'] == date_max]
            df_last_months = df_last_months.append(
                tmp_df, sort=False, ignore_index=True)

    # 3-- Get last year
    last_year = str(int(current_date_max[:4]) - 1)
    df_last_year = df_last[df_last['YEAR'] == last_year]
    last_year_date_max = df_last_year['SORT'].max()
    if last_year_date_max == last_week:
        df_last_year = pd.DataFrame()
    else:
        df_last_year = df_last_year[df_last_year['SORT'] == last_year_date_max]

    # create column period
    # db_final = df_current.append(df_last_week, sort=False, ignore_index=True).append(df_last_year, sort=False, ignore_index=True)
    db_final = pd.concat(
        [df_current, df_last_week, df_last_months, df_last_year])
    db_final['PERIOD'] = 'W' + db_final.loc[:, 'WEEK'].str[4:5] + ' ' + \
        db_final.loc[:, 'WEEK'].str[6:9] + ' ' + \
        db_final.loc[:, 'WEEK'].str[-4:]
    return db_final

# -- Fonction exclusion store : 
def exclusion_store(df):
    df = df[['CODE_JDA', 'LABEL_JDA', 'TYPOLOGY', 'REGION', 'ENTITY']]

    df_label = df[df['LABEL_JDA'] == 'Not Defined']
    df_entity = df[df['ENTITY'] == 'Not Defined']
    df_region = df[df['REGION'] == 'Not Defined']
    df_typo = df[df['TYPOLOGY'] == 'Not Defined']

    df_concat = pd.concat([df_label, df_entity, df_region, df_typo], axis=0)

    df_concat = df_concat.drop_duplicates().sort_values('CODE_JDA').reset_index(drop=True)
    return df_concat

# -- STEP2 : create db with scenario and calculate variation
def step2(db_init, df_par_var):
    # Filter on store 'not defined'
    db_init = db_init[(db_init['LABEL_JDA'] != 'Not Defined')
                      & (db_init['ENTITY'] != 'Not Defined')
                      & (db_init['REGION'] != 'Not Defined')
                      & (db_init['TYPOLOGY'] != 'Not Defined')]

    def cleaning(x):
        x = x.replace(',', '.')
        x = x.replace('(', '-')
        x = x.replace(')', '')
        x = x.replace(' ', '')
        return x

    db_var = {}
    db_var = pd.DataFrame(db_var, columns=['PERIOD', 'ENTITY', 'TYPOLOGY', 'COUNTRY',
                                           'REGION', 'CODE_JDA', 'LABEL_JDA', 'DEP_CODE', 'PERIMETER', 'VF', 'VI'])

    # loop on scenario (param)
    for _, param_r in df_par_var.iterrows():
        scenario = param_r['SCENARIO']
        perimeter = param_r['PERIMETER'] # Colonne ?
        vf = param_r['VF']
        vi = param_r['VI']
        columns = ['PERIOD', 'ENTITY', 'TYPOLOGY', 'COUNTRY', 'REGION',
                   'CODE_JDA', 'LABEL_JDA', 'DEP_CODE', perimeter, vf, vi]

        tmp_df = db_init[columns].rename(
            index=str, columns={perimeter: 'PERIMETER', vf: 'VF', vi: 'VI'})
        tmp_df['VF'] = tmp_df['VF'].astype(str).apply(
            lambda x: cleaning(x)).astype(float)
        tmp_df['VI'] = tmp_df['VI'].astype(str).apply(
            lambda x: cleaning(x)).astype(float)
        tmp_df['VARV'] = tmp_df['VF'] - tmp_df['VI']
        tmp_df['VARP'] = (tmp_df['VARV']) / np.abs(tmp_df['VI'])
        tmp_df['SCENARIO'] = scenario

        db_var = db_var.append(tmp_df, sort=False, ignore_index=True)

    return db_var

# -- STEP3 : Création des tables détaillées
def step3(db_var, df_map_depart, map_new):
    # table détaillée (departments) sans GLOBAL
    db_detail = pd.merge(db_var, df_map_depart.drop(
        ['DEP_LABEL'], axis=1), on='DEP_CODE')

    # create perimeter comparability
    db_detail1 = db_detail[(db_detail['TYPOLOGY'] == 'Stores') & (
        db_detail['PERIMETER'] == 'Comp')]
    db_detail['PERIMETER'] = 'Total'
    db_detail = db_detail.append(db_detail1)

    # add ENTITY CHILD
    df_concat = pd.merge(db_detail, map_new, left_on = 'ENTITY', right_on = 'ENTITIES')
    cols_to_drop = ['ORDER', 'DASHBOARD', 'ENTITIES']
    df_concat = drop_cols(df_concat, cols_to_drop)
    df_concat['ENTITY_CHILD'] = df_concat.apply(
        lambda row: row['REGION'] if row['RULES'] == 'ECOM' else row['COUNTRY'], axis=1)
    
    # table détaillée (stores + departments) avec GLOBAL
    col_wog = ['PERIOD', 'SCENARIO', 'ENTITY', 'ENTITY_CHILD','COUNTRY', 'TYPOLOGY', 'LABEL_JDA',
               'DEP_LABEL_N0', 'DEP_LABEL_N1', 'PERIMETER', 'VF', 'VI', 'VARV', 'VARP', 'RULES', 'REGION']
    db_agg_wog = df_concat[col_wog]

    col_wg = ['PERIOD', 'SCENARIO', 'ENTITY', 'TYPOLOGY',
              'LABEL_JDA', 'DEP_LABEL_N0', 'DEP_LABEL_N1', 'PERIMETER']
    db_agg_wg = df_concat.groupby(col_wg, as_index=False).agg(
        {'VF': 'sum', 'VI': 'sum', 'VARV': 'sum'}).rename(index=str, columns={'ENTITY': 'ENTITY_CHILD'})
    db_agg_wg['ENTITY'] = 'GLOBAL'
    db_agg_wg['VARP'] = (db_agg_wg['VARV']) / np.abs(db_agg_wg['VI'])

    db_agg = db_agg_wog.append(db_agg_wg, sort=False, ignore_index=True)
    return db_agg


# -- STEP4 : Création des tables intermédiaires
def step4_dep(db_agg, df_par_tab_agg):
    # Niveau fin par ENTITY
    columns1 = ["PERIOD", "SCENARIO", "ENTITY", "PERIMETER", "DEP_LABEL_N0"]
    db_tab1 = db_agg.groupby(columns1, as_index=False).agg(
        {'VF': 'sum', 'VI': 'sum', 'VARV': 'sum'}).rename(index=str, columns={'DEP_LABEL_N0': 'DEP_LABEL'})
    db_tab1['ENTITY_CHILD'] = 'TOTAL'
    db_tab1['DEP_LEVEL'] = 'N0'

    # Niveau détaillé par ENTITY
    columns2 = ["PERIOD", "SCENARIO", "ENTITY", "PERIMETER", "DEP_LABEL_N1"]
    db_tab2 = db_agg.groupby(columns2, as_index=False).agg(
        {'VF': 'sum', 'VI': 'sum', 'VARV': 'sum'}).rename(index=str, columns={'DEP_LABEL_N1': 'DEP_LABEL'})
    db_tab2['ENTITY_CHILD'] = 'TOTAL'
    db_tab2['DEP_LEVEL'] = 'N1'

    # Niveau détaillé par ENTITY_CHILD
    columns3 = ["PERIOD", "SCENARIO", "ENTITY",
                "PERIMETER", "DEP_LABEL_N0", "ENTITY_CHILD"]
    db_tab3 = db_agg.groupby(columns3, as_index=False).agg(
        {'VF': 'sum', 'VI': 'sum', 'VARV': 'sum'}).rename(index=str, columns={'DEP_LABEL_N0': 'DEP_LABEL'})
    db_tab3['DEP_LEVEL'] = 'N0'

    db_tab = db_tab1.append(db_tab2, sort=False, ignore_index=True).append(
        db_tab3, sort=False, ignore_index=True)
    db_tab['VARP'] = (db_tab['VARV']) / np.abs(db_tab['VI'])
    return db_tab

# -- STEP4 : Création des tables intermédiaires
def step4_typ(db_agg, df_par_tab_agg):
    tab1 = 'DB_TYP'
    columns = ["PERIOD", "SCENARIO", "ENTITY",
               "PERIMETER", "TYPOLOGY", "LABEL_JDA"]
    db_tab = db_detail(db_agg, df_par_tab_agg, tab1, columns)
    return db_tab

# -- STEP4 : Création des tables intermédiaires
def step4_geo(db_agg, df_par_tab_agg):
    tab1 = 'DB_GEO'
    columns = ["PERIOD", "SCENARIO", "ENTITY",
               "PERIMETER", "ENTITY_CHILD", "LABEL_JDA"]
    db_tab = db_detail(db_agg, df_par_tab_agg, tab1, columns)
    return db_tab


def db_detail(db_agg, df_par_tab_agg, tab, col):
    tab1 = tab
    columns = col
    db_tab = db_agg.groupby(columns, as_index=False).agg(
        {'VF': 'sum', 'VI': 'sum', 'VARV': 'sum'})
    db_tab['VARP'] = (db_tab['VARV']) / np.abs(db_tab['VI'])
    tmp_init = db_tab.copy()

    for _, r in df_par_tab_agg.iterrows():
        tab2 = r['TABLE']
        lab_agg = r['LABEL_AGG']
        val = r['VALUE']

        if tab1 == tab2:
            tmp_df = tmp_init.copy()
            tmp_df[lab_agg] = val
            tmp_df = tmp_df.groupby(columns, as_index=False).agg(
                {'VF': 'sum', 'VI': 'sum', 'VARV': 'sum'})
            tmp_df['VARP'] = (tmp_df['VARV']) / np.abs(tmp_df['VI'])
            db_tab = db_tab.append(tmp_df, sort=False, ignore_index=True)
            tmp_init = tmp_df.copy()

    return db_tab

# -- STEP5 : Création de la table TREND détaillée

# Get week by date extract to join with trend
def get_week(df):
    df = df[['__filename__', 'PERIOD']].drop_duplicates()
    df.loc[:, 'DATE_EXTRACT'] = df['__filename__'].str[-27:-17]
    df = df.drop(['__filename__'], axis=1).drop_duplicates()
    return df
    
# STEP5_CLEANING: Cleaning dataframe & merge with the histo 
def step5_clean(df, histo):
    # remove columns not used
    to_drop = ['ENTITY', 'TYPOLOGY', 'COUNTRY', 'REGION']
    df = drop_cols(df, to_drop)

    # cleaning value 
    df['VALUE'] = df['VALUE'].astype(str).apply(
        lambda x: x.replace(' ', '')).astype(int) / 1000

    # rename scenario
    list_scenario = {'Sales WCS Week (Value)': 'ACT',
                    'Sales WCS Week LY (Value)': 'LY',
                    'Sales Plan BDG (Value)': 'BDG',
                    'Sales Plan FC1 (Value)': 'FC1',
                    'Sales Plan FC2 (Value)': 'FC2'}
    df['SCENARIO'] = df['SCENARIO'].replace(list_scenario)

    # select last file
    df.loc[:, 'DATE_EXTRACT'] = df['__filename__'].str[-27:-17].astype(str)
    df_merge = pd.merge(df, histo, on='DATE_EXTRACT', how='left')
    return df_merge

# STEP5_CONSO : Store enrichment 
def step5_conso(df, df_store):
    # merge with the ref store
    df['CODE_JDA'] = df['CODE_JDA'].astype(str)
    df_store['CODE_JDA'] = df_store['CODE_JDA'].astype(str)
    db_init = pd.merge(df, df_store, on='CODE_JDA', how='left')
    
    # filter to remove "not defined" 
    db_init['LABEL_JDA'] =  db_init['LABEL_JDA'].fillna('Not Defined')
    db_init = db_init[db_init['LABEL_JDA'] != 'Not Defined']
    db_init = db_init[db_init['REGION'] != 'Not Defined']
    db_init = db_init[db_init['ENTITY'] != 'Not Defined']
    
    
    # group by date & remove store
    to_group = ['ENTITY', 'TYPOLOGY', 'REGION', 'COUNTRY',
                'PERIMETER', 'DATE', 'SCENARIO', '__filename__', 'PERIOD']
    df_conso = db_init.groupby(to_group, as_index=False).agg({'VALUE': 'sum'})
    return df_conso

# STEP5_SPEC : Calc th quarter & merge with the ref entity
def step5_spe(db_detail, map_new):
    # exclusion "Not defined"
    db_detail = db_detail[(db_detail['ENTITY'] != 'Not Defined')
                      & (db_detail['REGION'] != 'Not Defined')
                      & (db_detail['TYPOLOGY'] != 'Not Defined')]

    # calc DATE & QUARTER
    db_detail['WEEK'] = db_detail['DATE'].str[4:5]
    db_detail['MONTH'] = pd.to_datetime(
        db_detail['DATE'].str[6:], format='%B %Y').dt.strftime('%m')
    db_detail['INDEX'] = (db_detail['MONTH'] + ' - W' + db_detail['WEEK'])
    db_detail['INDEX_Q'] = (db_detail['MONTH'] + db_detail['WEEK']).astype(int)
    db_detail.loc[db_detail['INDEX_Q'] >= 101, 'QUARTER'] = 'Q4'
    db_detail.loc[db_detail['INDEX_Q'] < 101, 'QUARTER'] = 'Q3'
    db_detail.loc[db_detail['INDEX_Q'] < 71, 'QUARTER'] = 'Q2'
    db_detail.loc[db_detail['INDEX_Q'] < 41, 'QUARTER'] = 'Q1'
    db_detail['WEEKS'] = 'W' + db_detail['WEEK'] + ' ' + \
        db_detail['DATE'].str[6:9] + ' ' + db_detail['DATE'].str[-4:]
    db_detail['UNIT_VALUE'] = ' K€'

    # cleaning : sort & drop
    db_detail = db_detail.sort_values(by='INDEX_Q', ascending=True)
    to_drop = ['WEEK', 'MONTH']
    db_detail = drop_cols(db_detail, to_drop)
    
    #  cleaning ref entity
    map_new = map_new.rename(columns={'ENTITIES': 'ENTITY'})
    cols_to_drop = ['__filename__', 'domain', 'ORDER', 'DASHBOARD']
    map_new = drop_cols(map_new, cols_to_drop)

    # merge with ref entity
    df_merge = pd.merge(db_detail, map_new, on='ENTITY')
    return df_merge

# STEP5_RETAIL :
def step5_retail(df):
    # add ENTITY CHILD
    df_retail = df.copy()
    df_retail['ENTITY_CHILD'] = df_retail.apply(
        lambda row: row['REGION'] if row['RULES'] == 'ECOM' else row['COUNTRY'], axis=1)

    # create dataframe comp
    df_comp = df_retail.copy()
    df_comp = df_comp[(df_comp['TYPOLOGY'] == 'Stores') & (df_comp['PERIMETER'] == 'Comp')]

    # create dataframe total
    df_tot = df_retail.copy()
    df_tot.loc[:, 'PERIMETER'] = 'Total'

    # concat
    df_concat = pd.concat([df_comp, df_tot], axis=0, sort=False)

    # remove TYPOLOGY column
    to_drop = ['TYPOLOGY', 'RULES', 'COUNTRY', 'REGION']
    df_concat = drop_cols(df_concat, to_drop)

    # create "Total region"
    df_total = df_concat.copy()
    df_total['ENTITY_CHILD'] = 'Total Region'

    df_global = df_total.copy()
    df_global['ENTITY'] = 'GLOBAL'
    df_global['ENTITY_CHILD'] = 'GLOBAL' 

    df_conso = pd.concat([df_total, df_concat, df_global], axis=0, sort=False)

    # group by
    to_group = ['ENTITY', 'ENTITY_CHILD', 'SCENARIO', 'PERIMETER',
                'DATE', 'PERIOD', 'INDEX', 'INDEX_Q',
                'QUARTER', 'WEEKS', 'UNIT_VALUE']
    df_conso = df_conso.groupby(to_group, as_index=False).agg({'VALUE': 'sum'})

    return df_conso

# STEP5_ECOM : 
def step5_ecom(df):
    # cleaning
    df_ecom = df.copy()
    to_drop = ['RULES', 'PERIMETER', 'ENTITY']
    df_ecom = drop_cols(df_ecom, to_drop)
    
    #  rename
    df_ecom = df_ecom.rename(columns={'REGION': 'ENTITY'})
    to_replace = {'eCommerce Digital Concessions': 'Digital concessions'}
    df_ecom['TYPOLOGY'] = df_ecom['TYPOLOGY'].replace(to_replace)

    # Calc Brand Website & concat with principal dataframe
    df_tmp = df_ecom.copy()
    df_tmp = df_tmp[df_tmp['TYPOLOGY'].isin(['eCommerce Eagle', 'eCommerce Elite'])]
    df_tmp['TYPOLOGY'] = 'Brand Website'
    df_concat = pd.concat([df_ecom, df_tmp], axis=0, sort=False)

    # Filter on e-commerce typology 
    df_concat = df_concat[df_concat['TYPOLOGY'].isin(['Brand Website', 'Digital concessions'])]
 
    # create "Total region"
    df_total = df_concat.copy()
    df_total['COUNTRY'] = 'Total Region'
    
    # create "GLOBAL" entity
    df_global = df_total.copy()
    df_global['ENTITY'] = 'GLOBAL'
    df_global['COUNTRY'] = 'GLOBAL'
    
    df_conso = pd.concat([df_total, df_concat, df_global], axis=0, sort=False)

    # Total Digital sales
    df_bottom = df_conso.copy()
    df_bottom['TYPOLOGY'] = 'Total'
    df_final = pd.concat([df_bottom, df_conso], axis=0, sort=False)
    
    to_group = ['ENTITY', 'SCENARIO', 'COUNTRY', 'TYPOLOGY', 'DATE',
                'PERIOD', 'INDEX', 'INDEX_Q',
                'QUARTER', 'WEEKS', 'UNIT_VALUE']
    df_final = df_final.groupby(to_group, as_index=False).agg({'VALUE': 'sum'})
    return df_final


# -- STEP 6 : ECOM DB_ALL FILE: Calc Brand Website axis, filter on Ecom axis & calc total sales ecom
def step6_ecom(df):
    # Cleaning
    cols_to_drop = ['ENTITY_CHILD', 'ENTITY', 'VARP']
    df = drop_cols(df, cols_to_drop)
    df['ENTITY'] = df['REGION']

    # Rename 
    to_replace = {'eCommerce Digital Concessions': 'Digital concessions'}
    df['TYPOLOGY'] = df['TYPOLOGY'].replace(to_replace)

    # Calc GLOBAL on column ENTITY
    df_global = df.copy()
    df_global['ENTITY'] = 'GLOBAL'
    df_global = pd.concat([df, df_global], axis=0, sort=False)

    # Calc Brand Website & concat with principal dataframe
    df_tmp = df_global.copy()
    df_tmp = df_tmp[df_tmp['TYPOLOGY'].isin(['eCommerce Eagle', 'eCommerce Elite'])]
    df_tmp['TYPOLOGY'] = 'Brand Website'
    df_ecom = pd.concat([df_global, df_tmp], axis=0, sort=False)

    # Filter on e-commerce typology 
    df_ecom = df_ecom[df_ecom['TYPOLOGY'].isin(['Brand Website', 'Digital concessions'])]

    # Total Digital sales
    df_total = df_ecom.copy()
    df_total['TYPOLOGY'] = 'Total'
    df_final = pd.concat([df_ecom, df_total], axis=0, sort=False)

    to_group = ['PERIOD', 'SCENARIO', 'ENTITY', 'REGION', 'COUNTRY',
                'TYPOLOGY', 'LABEL_JDA','DEP_LABEL_N0',
                'DEP_LABEL_N1', 'PERIMETER', 'RULES']
    df_final = df_final.groupby(to_group, as_index=False)
    df_final = df_final.agg({'VF': 'sum', 'VI': 'sum', 'VARV': 'sum'})
    return df_final

# ----------------------------------------------------------------------------------------------------------------
# DATAMODELS
# ----------------------------------------------------------------------------------------------------------------

# --- Report :


def get_period(db_init):
    db_period = db_init[['PERIOD', 'WEEK', 'FILENAME']].drop_duplicates()
    db_period['FILE_TYPE'] = db_period['FILENAME'].str[7:8]
    db_period.loc[db_period['FILE_TYPE'] == 'M', 'FILE'] = 'Merch'
    db_period.loc[db_period['FILE_TYPE'] == 'C', 'FILE'] = 'Certify'
    db_period['DT_MAJ'] = pd.to_datetime(
        db_period['FILENAME'].str[-27:-8], format='%Y-%m-%d %H.%M.%S').dt.strftime('%d/%m/%Y %H:%M:%S')

    # Create ORDER column
    db_period['YEAR'] =  db_period['PERIOD'].str[7:]
    db_period['MONTH'] = db_period['PERIOD'].str[3:6]
    db_period['WEEK'] =  db_period['PERIOD'].str[1:3]
    db_period['MONTH'] = pd.to_datetime(db_period['MONTH'], format='%b').dt.strftime('%m').astype(str)
    db_period['ORDER'] =  db_period['YEAR'] + db_period['MONTH'] + db_period['WEEK']

    # Cleaning dataframe
    db_period = db_period.drop(['YEAR', 'WEEK', 'MONTH'], axis=1)
    return db_period

# --- Dashboard Heading I : Overview
# -- Actual Sales - Value dynamic (001)


def domain_001(db_geo, ws_021):
    domain = db_geo[(db_geo['PERIMETER'] == 'Total') &
                    (db_geo['ENTITY_CHILD'] == 'TOTAL')]
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'VALUE': domain['VF'],
        'VARV': domain['VARV'],
    })

    domain = domain.append(ws_021, sort=False, ignore_index=True)

    columns = ['PERIOD', 'SCENARIO', 'ENTITY']
    domain = domain.groupby(columns, as_index=False).agg(
        {'VALUE': 'sum', 'VARV': 'sum'})

    domain['LABEL'] = 'Total Sales'
    domain['VARP'] = domain['VARV'] / np.abs(domain['VALUE']-domain['VARV'])
    domain['UNIT_VALUE'] = ' K€'
    domain['UNIT_VAR'] = ' %'
    return domain

# -- Sales Distrubtion - Leaderboard (002)


def domain_002(tt_001, re_011, ws_021):
    ws_021['LABEL'] = 'Wholesale'
    ws_021['VARP'] = ws_021['VARV'] / np.abs(ws_021['VALUE']-ws_021['VARV'])
    ws_021['UNIT_VALUE'] = ' K€'
    ws_021['UNIT_VAR'] = ' %'

    domain = tt_001.append(re_011, sort=False, ignore_index=True).append(
        ws_021, sort=False, ignore_index=True)
    return domain

# -- Sales by Department - Leaderboard (003)


def domain_003(tt_001, re_016, ws_024):
    domain = re_016.drop(['VARP', 'UNIT_VALUE', 'UNIT_VAR'], axis=1).append(
        ws_024, sort=False, ignore_index=True)
    columns = ['PERIOD', 'SCENARIO', 'ENTITY', 'LABEL']
    domain = domain.groupby(columns, as_index=False).agg(
        {'VALUE': 'sum', 'VARV': 'sum'})
    domain['VARP'] = domain['VARV'] / np.abs(domain['VALUE']-domain['VARV'])
    domain['UNIT_VALUE'] = ' K€'
    domain['UNIT_VAR'] = ' %'

    domain = domain.append(tt_001, sort=False, ignore_index=True)
    return domain

# --- Dashboard Heading II : Retail
# -- Retail Sales - Value dynamic (011)


def domain_011(db_geo):
    domain = db_geo[(db_geo['PERIMETER'] == 'Total') &
                    (db_geo['ENTITY_CHILD'] == 'TOTAL')]
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': 'Retail',
        'VALUE': domain['VF'],
        'VARV': domain['VARV'],
        'VARP': domain['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain

# -- Retail Sales Comp - Value dynamic (012)


def domain_012(db_geo):
    domain = db_geo[(db_geo['PERIMETER'] == 'Comp') &
                    (db_geo['ENTITY_CHILD'] == 'TOTAL')]
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': 'Retail sales Comp',
        'VALUE': domain['VF'],
        'VARP': domain['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain

# -- Sales Trend - Linechart (013)


def domain_013(db_detail):
    domain = db_detail[(db_detail['PERIMETER'] == 'Total') & (db_detail['SCENARIO'] == 'ACT') & (
        db_detail['ENTITY_CHILD'].isin(['GLOBAL', 'Total Region']))]
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'INDEX': domain['INDEX'],
        'ENTITY': domain['ENTITY'],
        'DATE': domain['WEEKS'],
        'VALUE': domain['VALUE'],
        'UNIT_VALUE': ' K€',
    })
    return domain

# -- Sales by Geography - Leaderboard (014)


def domain_014(db_geo):
    domain = db_geo[(db_geo['PERIMETER'] == 'Total') & (
        db_geo['ENTITY_CHILD'] != 'TOTAL') & (db_geo['LABEL_JDA'] == 'TOTAL')]
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': domain['ENTITY_CHILD'],
        'VALUE': domain['VF'],
        'VARV': domain['VARV'],
        'VARP': domain['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain

# -- Sales by Typology - Leaderboard (015)


def domain_015(db_typ):
    domain = db_typ[(db_typ['PERIMETER'] == 'Total') & (
        db_typ['TYPOLOGY'] != 'Total Typology') & (db_typ['LABEL_JDA'] == 'TOTAL')]
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': domain['TYPOLOGY'],
        'VALUE': domain['VF'],
        'VARV': domain['VARV'],
        'VARP': domain['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain

# -- Sales by Department - Leaderboard (016)


def domain_016(db_dep):
    domain = db_dep[(db_dep['PERIMETER'] == 'Total')
                    & (db_dep['DEP_LEVEL'] == 'N1')]
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': domain['DEP_LABEL'],
        'VALUE': domain['VF'],
        'VARV': domain['VARV'],
        'VARP': domain['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain

# -- Sales by Department - Leaderboard (021)


def domain_021(ws_301):
    domain = ws_301[(ws_301['LABEL'] == 'TOTAL')]
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'VALUE': domain['VALUE'],
        'VARV': domain['VARV'],
        'VARP': domain['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain

# -- Sales by Department - Leaderboard (022)


def domain_022(ws_301):
    domain = ws_301[(ws_301['LABEL'] != 'TOTAL')]
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': domain['LABEL'],
        'VALUE': domain['VALUE'],
        'VARV': domain['VARV'],
        'VARP': domain['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain

# -- Sales by Season - Leaderboard (023)


def domain_023(ws_302):
    domain = ws_302[(ws_302['PACKS'] != 'TOTAL') &
                    (ws_302['UPPER_FILTER_1'] == 'All Collections') &
                    (ws_302['UPPER_FILTER_2'].isin(['All Regions', 'Total Region'])) &
                    (ws_302['PACKS'].astype(str).str.contains(
                        '(Worldwide|Total Europe|Total Asia|Total America).*'))
                    ]
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': domain['LABEL'],
        'VALUE': domain['VALUE'],
        'VARV': domain['VARV'],
        'VARP': domain['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain

# -- Sales by Department - Leaderboard (024)


def domain_024(ws_303):
    domain = ws_303[(ws_303['LABEL'] != 'TOTAL') &
                    (ws_303['UPPER_FILTER'].isin(['All Regions', 'Total Region'])) &
                    (ws_303['PACKS'].astype(str).str.contains(
                        '(Worldwide|Total Europe|Total Asia|Total America).*'))
                    ]
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': domain['LABEL'],
        'VALUE': domain['VALUE'],
        'VARV': domain['VARV'],
        'VARP': domain['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain

# --- Digital Sales - Value dynamic (031)
def domain_031(df):
    df = df[df['TYPOLOGY'] == 'Total' ]
    to_group = ['PERIOD', 'SCENARIO', 'ENTITY']
    domain = df.copy()
    domain = domain.groupby(to_group, as_index=False)
    domain = domain.agg({'VF': 'sum', 'VARV': 'sum', 'VI': 'sum'})
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': 'Digital',
        'VALUE': domain['VF'],
        'VARV': domain['VARV'],
        'VARP': domain['VARV'] / domain['VI'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain
# --- Focus Brand Website - Value dynamic (032)
def domain_032(df):
    df = df[df['TYPOLOGY'] == 'Brand Website' ]
    to_group = ['PERIOD', 'SCENARIO', 'ENTITY']
    domain = df.copy()
    domain = domain.groupby(to_group, as_index=False)
    domain = domain.agg({'VF': 'sum', 'VARV': 'sum', 'VI': 'sum'})
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': 'Digital',
        'VALUE': domain['VF'],
        'VARV': domain['VARV'],
        'VARP': domain['VARV'] / domain['VI'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain

# --- Sales trend - Linechart (033)
def domain_033(df):
    df = df[(df['TYPOLOGY'] == 'Total') 
            & (df['SCENARIO'] == 'ACT')  & (df['COUNTRY'].isin(['Total Region', 'GLOBAL']))]
    domain = df.copy() 
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'INDEX': domain['INDEX'],
        'ENTITY': domain['ENTITY'],
        'DATE': domain['WEEKS'],
        'VALUE': domain['VALUE'],
        'UNIT_VALUE': ' K€',
    })
    return domain

# --- Sales by geography - Leaderboard (034)
def domain_034(df):
    # create ENTITY_CHILD column
    domain = df.copy()
    domain['ENTITY_CHILD'] = domain.apply(
        lambda row: row['REGION'] if row['ENTITY'] == 'GLOBAL' else row['COUNTRY'], axis=1)
    domain = domain[domain['TYPOLOGY'] == 'Total' ]
    
    # create dataframe
    to_group = ['PERIOD', 'SCENARIO',
                     'ENTITY', 'ENTITY_CHILD']
    domain = domain.groupby(to_group, as_index=False)
    domain = domain.agg({'VF': 'sum', 'VARV': 'sum', 'VI': 'sum'})
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': domain['ENTITY_CHILD'],
        'VALUE': domain['VF'],
        'VARV': domain['VARV'],
        'VARP': domain['VARV'] / domain['VI'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain


# --- Sales by typology - Leaderboard (035)
def domain_035(df):
    df = df[df['TYPOLOGY'] != 'Total' ]
    to_group = ['PERIOD', 'SCENARIO',
                'ENTITY', 'TYPOLOGY']
    domain = df.copy()
    domain = domain.groupby(to_group, as_index=False)
    domain = domain.agg({'VF': 'sum', 'VARV': 'sum', 'VI': 'sum'})
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': domain['TYPOLOGY'],
        'VALUE': domain['VF'],
        'VARV': domain['VARV'],
        'VARP': domain['VARV'] / domain['VI'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain

# --- Sales by department - Leaderboard (036)
def domain_036(df):
    df = df[df['TYPOLOGY'] != 'Total' ]
    to_group = ['PERIOD', 'SCENARIO',
                'ENTITY', 'DEP_LABEL_N1']
    domain = df.copy()
    domain = domain.groupby(to_group, as_index=False)
    domain = domain.agg({'VF': 'sum', 'VARV': 'sum', 'VI': 'sum'})
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': domain['DEP_LABEL_N1'],
        'VALUE': domain['VF'],
        'VARV': domain['VARV'],
        'VARP': domain['VARV'] / domain['VI'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })
    return domain


# --- Chapter I : Overview

# Function : Split SCENARIO + CURRENT and create new units for HKPIS
def split_scenario(df):
    domain = df.copy()
    # Split SCENARIO.CURRENT
    domain['FREQUENCY'] = domain['SCENARIO'].str[:3]
    domain['COMPARISON'] = domain['SCENARIO'].str[3:]
    domain['VARP'] = domain['VARP'] * 100
    if not 'UNIT_VAR' in domain.columns:
        domain['UNIT_VAR'] = ' %'
    domain['UNIT_HKPIS'] = domain['UNIT_VAR'].str[1:] + domain['COMPARISON']
    return domain

# Function : Clean Data : exclude rows with value 0 and no variation
def clean_value(df):
    check_value = False
    check_varp = False

    if 'VALUE' in df.columns:
        check_value = True

    if 'VALUE' in df.columns:
        check_varp = True
    
    if check_value and check_varp:
        indexes = df.loc[(df['VALUE'] == 0) &
                         ((df['VARP']).isnull())].index
        df = df.drop(indexes)
    return df


#-- Retail + Wholesale (101)
def domain_101(tt_002):
    #GROUPS = ACT
    domain1 = tt_002.copy()
    domain1 = domain1[domain1['SCENARIO'].str.contains('.+ vs LY$')]
    domain1['GROUPS'] = 'ACT'

    #GROUPS = BDG / LY / FC1 / FC2
    domain2 = tt_002.copy()
    domain2['GROUPS'] = domain2['SCENARIO'].str[7:]
    domain2['SCENARIO'] = domain2['SCENARIO'].str[:7] + 'LY'
    domain2['VALUE'] = domain2['VALUE'] - domain2['VARV']

    domain = domain1.append(domain2, sort=False, ignore_index=True)

    # Pivot des variations en ligne pour créer UPPER_FILTER
    columns = ['ENTITY', 'SCENARIO', 'GROUPS', 'PERIOD', 'LABEL', 'VALUE']
    domain = pd.DataFrame.melt(domain,
                               id_vars=columns,
                               value_vars=['VARP', 'VARV'],
                               var_name='UPPER_FILTER',
                               value_name='VAR')
    domain['UNIT_VALUE'] = ' K€'
    domain.loc[domain['UPPER_FILTER'] == 'VARP', 'UNIT_VAR'] = ' %'
    domain.loc[domain['UPPER_FILTER'] == 'VARV', 'UNIT_VAR'] = ' K€'
    domain.loc[domain['UPPER_FILTER'] == 'VARP', 'UPPER_FILTER'] = 'Var in %'
    domain.loc[domain['UPPER_FILTER'] ==
               'VARV', 'UPPER_FILTER'] = 'Var in value'

    domain['Var vs LY'] = domain.loc[domain['GROUPS'] == 'LY', 'VAR']
    domain['Var vs BDG'] = domain.loc[domain['GROUPS'] == 'BDG', 'VAR']
    domain['Var vs FC1'] = domain.loc[domain['GROUPS'] == 'FC1', 'VAR']
    domain['Var vs FC2'] = domain.loc[domain['GROUPS'] == 'FC2', 'VAR']

    # Création des scenarios
    domain_BDG = domain.copy()
    domain_BDG['SCENARIO'] = domain_BDG['SCENARIO'].str[:7] + 'BDG'
    domain_FC1 = domain.copy()
    domain_FC1['SCENARIO'] = domain_FC1['SCENARIO'].str[:7] + 'FC1'
    domain_FC2 = domain.copy()
    domain_FC2['SCENARIO'] = domain_FC1['SCENARIO'].str[:7] + 'FC2'
    domain = domain.append(domain_BDG, sort=False, ignore_index=True).append(
        domain_FC1, sort=False, ignore_index=True).append(domain_FC2, sort=False, ignore_index=True)

    # Clean Data
    domain['INDEX'] = pd.to_datetime(domain['PERIOD'].str[3:6], format='%b').dt.strftime(
        '%m') + (domain['PERIOD'].str[1:2])
    domain['INDEX'] = domain['INDEX'].astype(int)

    indexes = domain.loc[(domain['GROUPS'] == 'BDG') &
                         (domain['INDEX'] > 64)].index
    domain = domain.drop(indexes)
    indexes = domain.loc[(domain['GROUPS'] == 'FC1') &
                         (domain['INDEX'] < 51)].index
    domain = domain.drop(indexes)
    indexes = domain.loc[(domain['GROUPS'] == 'FC2') &
                         (domain['INDEX'] < 111)].index
    domain = domain.drop(indexes)

    # send message to healthcheck if done
    hc_done(hc_key)
    return domain


# -- Sales bridge (102)
def domain_102(tt_001, re_014, ws_022, re_016, ws_024, param):
    # Total Sales
    domain = tt_001.copy()
    domain = domain.drop(['LABEL', 'UNIT_VALUE', 'UNIT_VAR'], axis=1).rename(
        index=str, columns={'VALUE': 'ACT'})
    domain['VI'] = domain['ACT'] - domain['VARV']

    # Pivot des valeurs en ligne
    columns = ['ENTITY', 'SCENARIO', 'PERIOD']
    domain = pd.DataFrame.melt(domain,
                               id_vars=columns,
                               value_vars=['ACT', 'VI'],
                               var_name='LABEL',
                               value_name='VALUE')
    domain.loc[domain['LABEL'] == 'VI', 'ORDER'] = 0
    domain.loc[domain['LABEL'] == 'ACT', 'ORDER'] = 100
    domain.loc[domain['LABEL'] == 'VI', 'LABEL'] = domain['SCENARIO'].str[7:]

    # FILTER : BY GEOGRAPHY
    # Total Sales
    domain1 = domain.copy()
    domain1['UPPER_FILTER'] = 'BY GEOGRAPHY'

    # Retail
    domain2 = re_014.drop(['VALUE', 'UNIT_VALUE', 'UNIT_VAR'], axis=1
                          ).rename(index=str, columns={'VARV': 'VALUE', 'LABEL': 'SUB_LABEL'})
    domain2['LABEL'] = 'RETAIL'
    domain2['UPPER_FILTER'] = 'BY GEOGRAPHY'

    # Wholesale
    domain3 = ws_022.drop(['VALUE', 'UNIT_VALUE', 'UNIT_VAR'], axis=1
                          ).rename(index=str, columns={'VARV': 'VALUE', 'LABEL': 'SUB_LABEL'})
    domain3['LABEL'] = 'WHOLESALE'
    domain3['UPPER_FILTER'] = 'BY GEOGRAPHY'

    domain_geo = domain2.append(domain3, sort=False, ignore_index=True)
    domain_geo = pd.merge(domain_geo, param.rename(index=str, columns={
                          'UPPER_FILTER': 'SUB_LABEL'}), on=['ENTITY', 'SUB_LABEL'])
    domain_geo = domain_geo.append(
        domain1, sort=False, ignore_index=True).sort_values(by='ORDER', ascending=True)

    # FILTER : BY PRODUCT
    # Total Sales
    domain1 = domain.copy()
    domain1['UPPER_FILTER'] = 'BY DEPARTMENT'

    # Retail
    domain2 = re_016.drop(['VALUE', 'UNIT_VALUE', 'UNIT_VAR'], axis=1
                          ).rename(index=str, columns={'VARV': 'VALUE', 'LABEL': 'SUB_LABEL'})
    domain2['LABEL'] = 'RETAIL'
    domain2['UPPER_FILTER'] = 'BY DEPARTMENT'

    # Wholesale
    domain3 = ws_024.drop(['VALUE', 'UNIT_VALUE', 'UNIT_VAR'], axis=1
                          ).rename(index=str, columns={'VARV': 'VALUE', 'LABEL': 'SUB_LABEL'})
    domain3['LABEL'] = 'WHOLESALE'
    domain3['UPPER_FILTER'] = 'BY DEPARTMENT'

    domain_dep = domain2.append(domain3, sort=False, ignore_index=True)
    domain_dep = pd.merge(domain_dep, param.drop('ENTITY', axis=1).rename(
        index=str, columns={'UPPER_FILTER': 'SUB_LABEL'}), on=['SUB_LABEL'])
    domain_dep = domain_dep.append(
        domain1, sort=False, ignore_index=True).sort_values(by='ORDER', ascending=True)

    domain = domain_geo.append(domain_dep, sort=False, ignore_index=True)
    return domain

# --- Chapter II : Retail

# --- Sub-Chapter I : Total Retail

# -- Sales by geography - Horizontal Barchart (201)
def domain_201(db_geo):
    # FILTER : 'All Countries' for Regions / 'All Regions' for GLOBAL
    domain1 = db_geo.copy()
    domain1 = domain1[(domain1['LABEL_JDA'] == 'TOTAL')]
    domain1 = pd.DataFrame({
        'PERIOD': domain1['PERIOD'],
        'SCENARIO': domain1['SCENARIO'],
        'ENTITY': domain1['ENTITY'],
        'LABEL': domain1['ENTITY_CHILD'],
        'UPPER_FILTER': domain1.apply(lambda row: 'All Countries' if row['ENTITY'] != 'GLOBAL' else 'All Regions', axis=1),
        'BOTTOM_FILTER': domain1['PERIMETER'],
        'VALUE': domain1['VF'],
        'VARP': domain1['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })

    # FILTER : Detailed Countries for Regions
    domain2 = db_geo.copy()
    domain2 = domain2[(domain2['ENTITY_CHILD'] != 'TOTAL')
                     & (domain2['ENTITY'] != 'GLOBAL')]
    domain2 = pd.DataFrame({
        'PERIOD': domain2['PERIOD'],
        'SCENARIO': domain2['SCENARIO'],
        'ENTITY': domain2['ENTITY'],
        'LABEL': domain2['LABEL_JDA'],
        'UPPER_FILTER': domain2['ENTITY_CHILD'],
        'BOTTOM_FILTER': domain2['PERIMETER'],
        'VALUE': domain2['VF'],
        'VARP': domain2['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })

    # FILTER : 'All Countries' for GLOBAL
    # détail All countries
    domain3a = domain1.copy()
    domain3a = domain3a[(domain3a['ENTITY'] != 'GLOBAL')
                       & (domain3a['LABEL'] != 'TOTAL')]
    domain3a.loc[:, 'ENTITY'] = 'GLOBAL'

    # total All countries
    domain3b = domain1.copy()
    domain3b = domain3b[(domain3b['ENTITY'] == 'GLOBAL')
                       & (domain3b['LABEL'] == 'TOTAL')]
    domain3b.loc[:, 'UPPER_FILTER'] = 'All Countries'

    # concat all domain
    domain = pd.concat([domain1, domain2, domain3a, domain3b], axis=0, sort=False)

    # rename 'REGION' for Ecommerce Brand Website when ENTITY = 'GLOBAL' and filter "All Countries"
    domain['LABEL'] = domain.apply(lambda row: {
        'AMERICA': "Elite America",
        'EUROPE': "Elite Europe",
        'MIDDLE EAST': 'Elite Middle East',
        'APAC': 'Elite Asia Pacific',
    }.get(row['LABEL'], row['LABEL']) if row['ENTITY'] == 'GLOBAL' and row['UPPER_FILTER'] == 'All Countries' else row['LABEL'], axis=1)
    
    # Clean Data : exclude rows with value 0 and no variation
    domain = clean_value(domain)
    # Function : Split SCENARIO + CURRENT and create new units for HKPIS
    domain = split_scenario(domain)
    return domain


def domain_201_upper(re_201, param):
    requester = re_201[['ENTITY', 'UPPER_FILTER']].drop_duplicates(
    ).sort_values(by='UPPER_FILTER', ascending=True)
    requester = pd.merge(requester, param, on=['ENTITY', 'UPPER_FILTER']).sort_values(
        by='ORDER', ascending=True)
    return requester


# -- Sales by typology- Horizontal Barchart (202)
def domain_202(db_typ):
    # FILTER : ALL TYPOLOGY with aggregation
    domain1 = db_typ
    domain1 = pd.DataFrame({
        'PERIOD': domain1['PERIOD'],
        'SCENARIO': domain1['SCENARIO'],
        'ENTITY': domain1['ENTITY'],
        'LABEL': domain1['LABEL_JDA'],
        'UPPER_FILTER': domain1['TYPOLOGY'],
        'BOTTOM_FILTER': domain1['PERIMETER'],
        'VALUE': domain1['VF'],
        'VARP': domain1['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })

    # FILTER : ALL TYPOLOGY without aggregation
    domain2 = db_typ[(db_typ['LABEL_JDA'] == 'TOTAL') &
                     (db_typ['TYPOLOGY'] != 'Total Typology')]
    domain2 = pd.DataFrame({
        'PERIOD': domain2['PERIOD'],
        'SCENARIO': domain2['SCENARIO'],
        'ENTITY': domain2['ENTITY'],
        'LABEL': domain2['TYPOLOGY'],
        'UPPER_FILTER': 'Total Typology',
        'BOTTOM_FILTER': domain2['PERIMETER'],
        'VALUE': domain2['VF'],
        'VARP': domain2['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })

    domain = domain1.append(domain2, sort=False, ignore_index=True)

    # Clean Data : exclude rows with value 0 and no variation
    domain = clean_value(domain)
    # Function : Split SCENARIO + CURRENT and create new units for HKPIS
    domain = split_scenario(domain)
    return domain


def domain_202_upper(re_202, param):
    requester = re_202[['ENTITY', 'UPPER_FILTER']].drop_duplicates()
    requester = pd.merge(requester, param.drop(
        ['ENTITY'], axis=1), on='UPPER_FILTER').sort_values(by='ORDER', ascending=True)
    return requester


# -- Sales by department- Horizontal Barchart (203)
def domain_203(db_dep, db_geo):
    # FILTER : TOTAL BY ENTITY
    domain1 = db_dep[(db_dep['ENTITY_CHILD'] == 'TOTAL')
                     & (db_dep['DEP_LEVEL'] == 'N0')]
    domain1 = pd.DataFrame({
        'PERIOD': domain1['PERIOD'],
        'SCENARIO': domain1['SCENARIO'],
        'ENTITY': domain1['ENTITY'],
        'LABEL': domain1['DEP_LABEL'],
        'UPPER_FILTER': domain1.apply(lambda row: 'All Regions' if row['ENTITY'] == 'GLOBAL' else 'Total Region', axis=1),
        'BOTTOM_FILTER': domain1['PERIMETER'],
        'VALUE': domain1['VF'],
        'VARP': domain1['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %'
    })

    # FILTER : DETAIL COUNTRIES BY REGION (OR GLOBAL)
    domain2 = db_dep[(db_dep['ENTITY_CHILD'] != 'TOTAL') & (
        db_dep['DEP_LEVEL'] == 'N0') & (db_dep['ENTITY'] != 'GLOBAL')]
    domain2 = pd.DataFrame({
        'PERIOD': domain2['PERIOD'],
        'SCENARIO': domain2['SCENARIO'],
        'ENTITY': domain2['ENTITY'],
        'LABEL': domain2['DEP_LABEL'],
        'UPPER_FILTER': domain2['ENTITY_CHILD'],
        'BOTTOM_FILTER': domain2['PERIMETER'],
        'VALUE': domain2['VF'],
        'VARP': domain2['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })

    # HKPIS : TOTAL
    def get_upper_filter(row1, row2):
        if row1 != 'TOTAL':
            return row1
        if row1 == 'TOTAL' and row2 == 'GLOBAL':
            return 'All Regions'
        if row1 == 'TOTAL' and row2 != 'GLOBAL':
            return 'Total Region'

    domain3a = db_geo[(db_geo['LABEL_JDA'] == 'TOTAL')
                      & (db_geo['ENTITY'] != 'GLOBAL')]
    domain3b = db_geo[(db_geo['LABEL_JDA'] == 'TOTAL') & (
        db_geo['ENTITY'] == 'GLOBAL') & (db_geo['ENTITY_CHILD'] == 'TOTAL')]
    domain3 = domain3a.append(domain3b, sort=False, ignore_index=True)

    domain3 = pd.DataFrame({
        'PERIOD': domain3['PERIOD'],
        'SCENARIO': domain3['SCENARIO'],
        'ENTITY': domain3['ENTITY'],
        'LABEL': domain3['LABEL_JDA'],
        'UPPER_FILTER': domain3.apply(lambda row: get_upper_filter(row['ENTITY_CHILD'], row['ENTITY']), axis=1),
        'BOTTOM_FILTER': domain3['PERIMETER'],
        'VALUE': domain3['VF'],
        'VARP': domain3['VARP'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })

    domain = domain1.append(domain2, sort=False, ignore_index=True).append(
        domain3, sort=False, ignore_index=True)

    # Clean Data : exclude rows with value 0 and no variation
    domain = clean_value(domain)
    # Function : Split SCENARIO + CURRENT and create new units for HKPIS
    domain = split_scenario(domain)
    return domain


def domain_203_upper(re_203, param):
    requester = re_203[['ENTITY', 'UPPER_FILTER']].drop_duplicates()
    requester = pd.merge(requester, param, on=['ENTITY', 'UPPER_FILTER']).sort_values(
        by='ORDER', ascending=True)
    return requester

# -- Sales trend - Linechart (204)
def domain_204(domain):
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'INDEX': domain['INDEX'],
        'INDEX_Q': domain['INDEX_Q'],
        'QUARTER': domain['QUARTER'],
        'ENTITY': domain['ENTITY'],
        'DATE': domain['WEEKS'].str[:-5],
        'UPPER_FILTER': domain['ENTITY_CHILD'],
        'UPPER_FILTER_2': domain['SCENARIO'],
        'BOTTOM_FILTER': domain['PERIMETER'],
        'VALUE': domain['VALUE'],
        'UNIT_VALUE': domain['UNIT_VALUE'],
    })

    # Clean Data
    indexes = domain.loc[(domain['UPPER_FILTER_2'] == 'BDG')
                         & (domain['INDEX_Q'] > 64)].index
    domain = domain.drop(indexes)

    indexes = domain.loc[(domain['UPPER_FILTER_2'] == 'FC1')
                         & (domain['INDEX_Q'] < 51)].index
    domain = domain.drop(indexes)

    indexes = domain.loc[(domain['UPPER_FILTER_2'] == 'FC2')
                         & (domain['INDEX_Q'] < 111)].index
    domain = domain.drop(indexes)

    return domain


def domain_204_upper(re_204, param):
    requester = re_204[['ENTITY', 'UPPER_FILTER']].drop_duplicates()
    requester = pd.merge(requester, param, on=['ENTITY', 'UPPER_FILTER']).sort_values(
        by='ORDER', ascending=True)
    return requester


# --- Sub-Chapter II : Focus Digital

# -- Sales by geography - Horizontal Barchart(211)
def domain_211(df):
    # Create 'All Countries' 
    def upper_all(df, label, upper_filter):
        to_group = ['PERIOD', 'SCENARIO', 'ENTITY', 'TYPOLOGY', label]
        domain = df.copy()
        domain = domain.groupby(to_group, as_index=False)
        domain = domain.agg({'VF': 'sum', 'VARV': 'sum', 'VI': 'sum'})
        domain = pd.DataFrame({
            'PERIOD': domain['PERIOD'],
            'SCENARIO': domain['SCENARIO'],
            'ENTITY': domain['ENTITY'],
            'LABEL': domain[label],
            'UPPER_FILTER': upper_filter,
            'BOTTOM_FILTER': domain['TYPOLOGY'],
            'VALUE': domain['VF'],
            'VARV': domain['VARV'],
            'VI': domain['VI'],
        })
        return domain

    df_region = upper_all(df, 'COUNTRY', 'All Countries')
    df_country = upper_all(df, 'REGION', 'All Regions')
    
    # Create countries in upper filter
    df_store = df.copy()
    df_store = df_store[df_store['ENTITY'] != 'GLOBAL']
    to_group = ['PERIOD', 'SCENARIO', 'ENTITY', 'LABEL_JDA', 'COUNTRY', 'TYPOLOGY']
    df_store = df_store.groupby(to_group, as_index=False)
    df_store = df_store.agg({'VF': 'sum', 'VARV': 'sum', 'VI': 'sum'})
    df_store = pd.DataFrame({
        'PERIOD': df_store['PERIOD'],
        'SCENARIO': df_store['SCENARIO'],
        'ENTITY': df_store['ENTITY'],
        'LABEL': df_store['LABEL_JDA'],
        'UPPER_FILTER': df_store['COUNTRY'],
        'BOTTOM_FILTER': df_store['TYPOLOGY'],
        'VALUE': df_store['VF'],
        'VARV': df_store['VARV'],
        'VI': df_store['VI'],
    })

    # Conso all dataframe
    df_conso = pd.concat([df_region, df_country, df_store], axis=0, sort=False)

    # Aggregation & Create unit
    to_group = ['PERIOD', 'SCENARIO', 'ENTITY', 'LABEL', 'UPPER_FILTER', 'BOTTOM_FILTER']
    df_conso = df_conso.groupby(to_group, as_index=False)
    df_conso = df_conso.agg({'VALUE': 'sum', 'VARV': 'sum', 'VI': 'sum'})
    df_conso['UNIT_VALUE'] = ' K€'
    df_conso['UNIT_VAR'] = ' %'

    # Calc "HKPIs"
    df_hkpis = df_conso.copy()
    df_hkpis['LABEL'] = 'TOTAL'
    to_group = ['PERIOD', 'SCENARIO', 'ENTITY',
                'LABEL', 'UPPER_FILTER', 'BOTTOM_FILTER',
                'UNIT_VALUE', 'UNIT_VAR']
    df_hkpis = df_hkpis.groupby(to_group, as_index=False)
    df_hkpis = df_hkpis.agg({'VALUE': 'sum', 'VARV': 'sum', 'VI': 'sum'})

    # Concat db conso + calculated Hkpis
    domain = pd.concat([df_hkpis, df_conso], axis=0, sort=False)
    
    # Calculate variation
    domain['VARP'] = domain['VARV'] / domain['VI']
    domain = domain.drop(['VI', 'VARV'], axis=1)

    # Clean Data : exclude rows with value 0 and no variation
    # domain = clean_value(domain)
    # Function : Split SCENARIO + CURRENT and create new units for HKPIS
    domain = split_scenario(domain)
    return domain

# --- Sales by department - Horizontal Barchart(212)
def domain_212(df):
    # UPPER FILTER + BOTTOM FILTER
    df.loc[df['ENTITY'] == 'GLOBAL', 'COUNTRY'] = 'All Regions'
    cols_to_group = ['PERIOD', 'SCENARIO', 'ENTITY',
                     'COUNTRY', 'TYPOLOGY', 'DEP_LABEL_N0']
    domain = df.groupby(cols_to_group, as_index=False)
    domain = domain.agg({'VF': 'sum', 'VARV': 'sum', 'VI': 'sum'})
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'SCENARIO': domain['SCENARIO'],
        'ENTITY': domain['ENTITY'],
        'LABEL': domain['DEP_LABEL_N0'],
        'UPPER_FILTER': domain['COUNTRY'],
        'BOTTOM_FILTER': domain['TYPOLOGY'],
        'VALUE': domain['VF'],
        'VARV': domain['VARV'],
        'VI': domain['VI'],
        'UNIT_VALUE': ' K€',
        'UNIT_VAR': ' %',
    })

    # Calc "Total region"
    df_tmp = domain.copy()
    df_tmp = df_tmp[(df_tmp['ENTITY'] != 'GLOBAL')]
    df_tmp['UPPER_FILTER'] = 'Total Region'
    to_group = ['PERIOD', 'SCENARIO', 'ENTITY', 'BOTTOM_FILTER',
                     'UPPER_FILTER', 'LABEL', 'UNIT_VAR', 'UNIT_VALUE']
    df_tmp = df_tmp.groupby(to_group, as_index=False)
    df_tmp = df_tmp.agg({'VALUE': 'sum', 'VARV': 'sum', 'VI': 'sum'})

    df_concat = pd.concat([domain, df_tmp], axis=0, sort=False)

    # Calc "HKPIs"
    df_hkpis = df_concat.copy()
    df_hkpis['LABEL'] = 'TOTAL'
    to_group = ['PERIOD', 'SCENARIO', 'ENTITY',
                'LABEL','UPPER_FILTER', 'BOTTOM_FILTER',
                'UNIT_VALUE', 'UNIT_VAR']
    df_hkpis = df_hkpis.groupby(to_group, as_index=False)
    df_hkpis = df_hkpis.agg({'VALUE': 'sum', 'VARV': 'sum', 'VI': 'sum'})
    
    # Concat db concat + calculated Hkpis
    domain = pd.concat([df_hkpis, df_concat], axis=0, sort=False)
    
    # Calculate variation
    domain['VARP'] = domain['VARV'] / domain['VI']
    domain = domain.drop(['VI', 'VARV'], axis=1)

    # Clean Data : exclude rows with value 0 and no variation
    # domain = clean_value(domain)
    # Function : Split SCENARIO + CURRENT and create new units for HKPIS
    domain = split_scenario(domain)
    return domain

# --- Sales trend - Linechart (213)
def domain_213(domain):
    domain = pd.DataFrame({
        'PERIOD': domain['PERIOD'],
        'INDEX': domain['INDEX'],
        'INDEX_Q': domain['INDEX_Q'],
        'QUARTER': domain['QUARTER'],
        'ENTITY': domain['ENTITY'],
        'DATE': domain['WEEKS'].str[:-5],
        'UPPER_FILTER': domain['COUNTRY'],
        'UPPER_FILTER_2': domain['SCENARIO'],
        'BOTTOM_FILTER': domain['TYPOLOGY'],
        'VALUE': domain['VALUE'],
        'UNIT_VALUE': domain['UNIT_VALUE'],
    })

    # Clean Data
    indexes = domain.loc[(domain['UPPER_FILTER_2'] == 'BDG')
                         & (domain['INDEX_Q'] > 64)].index
    domain = domain.drop(indexes)

    indexes = domain.loc[(domain['UPPER_FILTER_2'] == 'FC1')
                         & (domain['INDEX_Q'] < 51)].index
    domain = domain.drop(indexes)

    indexes = domain.loc[(domain['UPPER_FILTER_2'] == 'FC2')
                         & (domain['INDEX_Q'] < 111)].index
    domain = domain.drop(indexes)
    return domain.reset_index(drop=True)


# -- Sales by geography - Horizontal Barchart (301)
def domain_301(db_301):
    columns = ["PERIOD", "SCENARIO", "ENTITY"]
    domain = db_301.groupby(columns, as_index=False).agg(
        {'VALUE': 'sum', 'VARV': 'sum'})
    domain['PACKS'] = 'TOTAL'
    domain['LABEL'] = 'TOTAL'

    domain = domain.append(db_301, sort=False, ignore_index=True)
    domain['VARP'] = (domain['VARV']) / \
        np.abs(domain['VALUE'] - domain['VARV'])
    return split_scenario(domain)


# -- Sales by season - Horizontal Barchart (302)
def domain_302(db_302):
    domain1 = db_302[~db_302['PACKS'].str.contains('^Total.+')]
    domain2 = db_302[(db_302['PACKS'].isin(['Total America', 'Total APAC']))]
    domain = domain1.append(domain2, sort=False, ignore_index=True)

    columns1 = ["PERIOD", "SCENARIO", "ENTITY",
                "UPPER_FILTER_1", "UPPER_FILTER_2"]
    domain1 = domain.groupby(columns1, as_index=False).agg(
        {'VALUE': 'sum', 'VARV': 'sum'})
    domain1['PACKS'] = 'TOTAL'
    domain1['LABEL'] = 'TOTAL'

    domain = db_302.append(domain1, sort=False, ignore_index=True)
    domain['VARP'] = (domain['VARV']) / \
        np.abs(domain['VALUE'] - domain['VARV'])
    return split_scenario(domain)


def domain_302_upper(ws_302, param):
    # Upper filter right
    requester_r = ws_302.copy()[['ENTITY', 'UPPER_FILTER_2']].drop_duplicates().rename(
        index=str, columns={'UPPER_FILTER_2': 'UPPER_FILTER'})

    requester_r = pd.merge(requester_r, param, on=['ENTITY', 'UPPER_FILTER'])
    requester_r = requester_r.sort_values(by='ORDER', ascending=True)
    to_rename = {'ENTITY': 'FILTER'}
    requester_r = requester_r.rename(columns=to_rename)
    requester_r['TYPE'] = 'UPPER_FILTER_R'

    # Upper filter middle
    requester_m = ws_302.copy()[['UPPER_FILTER_1', 'UPPER_FILTER_2']].drop_duplicates()
    to_rename = {'UPPER_FILTER_1': 'UPPER_FILTER',
                 'UPPER_FILTER_2': 'FILTER'}
    requester_m = requester_m.rename(columns=to_rename)
    requester_m['TYPE'] = 'UPPER_FILTER_M'

    # Concat
    requester = pd.concat([requester_r, requester_m], axis=0)
    return requester


# -- Sales by department - Horizontal Barchart (303)
def domain_303(db_303):
    domain = db_303.copy()
    domain = domain[~(domain['PACKS'].astype(str).str.contains(
        '^Total (Europe|Asia|Other America).*'))]
    
    columns = ["PERIOD", "SCENARIO", "ENTITY", "UPPER_FILTER"]
    domain1 = domain.groupby(columns, as_index=False).agg(
        {'VALUE': 'sum', 'VARV': 'sum'})
    domain1['PACKS'] = 'TOTAL'
    domain1['LABEL'] = 'TOTAL'

    domain = db_303.append(domain1, sort=False, ignore_index=True)
    domain['VARP'] = (domain['VARV']) / \
        np.abs(domain['VALUE'] - domain['VARV'])
    return split_scenario(domain)


def domain_303_upper(ws_303, param):
    requester = ws_303[['ENTITY', 'UPPER_FILTER']].drop_duplicates()
    requester = pd.merge(requester, param, on=['ENTITY', 'UPPER_FILTER']).sort_values(
        by='ORDER', ascending=True)
    return requester


# -- Client Ranking - Horizontal Barchart (304)
def domain_304(db_304):
    domain = db_304.copy()
    columns = ["PERIOD", "SCENARIO", "ENTITY"]
    domain1 = domain.groupby(columns, as_index=False).agg(
        {'VALUE': 'sum', 'VARV': 'sum'})
    domain1['PACKS'] = 'TOTAL'
    domain1['LABEL'] = 'TOTAL'

    domain = db_304.append(domain1, sort=False, ignore_index=True)
    domain['VARP'] = (domain['VARV']) / \
        np.abs(domain['VALUE'] - domain['VARV'])
    return split_scenario(domain)