# -*- coding: utf-8 -*-
"""
CPS microdata to database compiler

@author: Gabriel Moss
"""
import pandas as pd
import numpy as np
import sqlite3
from sqlite3 import Error
import re
import requests
import zipfile
import getpass
import os
import glob
import pickle

def month_switch(mo):
    '''
    switcher dict for converting from month number to name
    
    :param mo: month number (int)
    '''
    switcher = {
            1: "jan",
            2: "feb",
            3: "mar",
            4: "apr",
            5: "may",
            6: "jun",
            7: "jul",
            8: "aug",
            9: "sep",
            10: "oct",
            11: "nov",
            12: "dec"
        }
    return(switcher.get(mo))

def get_raw_data(start_year_4_dig,end_year_4_dig):
    '''
    grabs zip files from census, stores them in file on desktop and unzips them
    to secondary file as .dat files. also grabs data dictionary for current year
    and stores that with files.
    
    :param start_year_4_dig: starting year for range observed (int)
    :param end_year_4_dig: ending year for range observed (int)
    '''
    
    for year in range(int(start_year_4_dig),int(end_year_4_dig)+1):
        yr = int(str(year)[2:4])
        
        yearpath_uz = newpath + '/cps_' + str(yr)
        if not os.path.exists(yearpath_uz):
            os.makedirs(yearpath_uz)
        yearpath_z = outpath + 'cps_' + str(yr) + '_ziped/'
        if not os.path.exists(yearpath_z):
            os.makedirs(yearpath_z)
        
        
        #check year, define extension
        #list of months
        list_test = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
        
        #loop that takes data from CPS and dowloads to computer
        for mo in range(1,len(list_test)+1):
            #most recent data dictionary
            dat_dict = 'https://www2.census.gov/programs-surveys/cps/datasets/2020/basic/2020_Basic_CPS_Public_Use_Record_Layout_plus_IO_Code_list.txt'
            
            month = month_switch(mo)
            url = "https://www2.census.gov/programs-surveys/cps/datasets/{}/basic/{}{}pub.zip".format(year,month,yr)
            outfname = yearpath_z + url.split('/')[-1]
            out_text_name = yearpath_uz + '/' + dat_dict.split('/')[-1]

            try:
                r = requests.get(url, stream=True)
                if(r.status_code == requests.codes.ok):
                    with open(outfname,"wb") as fd:
                        for chunk in r.iter_content(chunk_size=1024):
                            if chunk:
                                fd.write(chunk)
                        fd.close()
                with zipfile.ZipFile('{}{}{}pub.zip'.format(yearpath_z,month,yr),'r') as zip_ref:
                    zip_ref.extractall(yearpath_uz)
            except:
                print(month + " " + str(year) + " data is unavaiable at this time")
            try:
                r = requests.get(dat_dict, stream=True)
                if(r.status_code == requests.codes.ok):
                    with open(out_text_name,"wb") as fd:
                        for chunk in r.iter_content(chunk_size=1024):
                            if chunk:
                                fd.write(chunk)
                        fd.close()
            except:
                np.nan
        
        #add in extract files for cert and lic for 2015 and 2016
        if year in [2015,2016]:
            dat_dict2 = 'https://www2.census.gov/programs-surveys/cps/datasets/2015/supp/Certification_extract_file_{}_rec_layout.txt'.format(year)
            url2 = 'https://www2.census.gov/programs-surveys/cps/datasets/2015/supp/jan{}-dec{}cert_ext.zip'.format(yr,yr)
            outfname2 = yearpath_z + url2.split('/')[-1]
            out_text_name2 = yearpath_uz + '/' + dat_dict2.split('/')[-1]
            
            try:
                r = requests.get(url2, stream=True)
                if(r.status_code == requests.codes.ok):
                    with open(outfname2,"wb") as fd:
                        for chunk in r.iter_content(chunk_size=1024):
                            if chunk:
                                fd.write(chunk)
                        fd.close()
                with zipfile.ZipFile(outfname2,'r') as zip_ref:
                    zip_ref.extractall(yearpath_uz)
            except:
                print("{} data is unavaiable at this time".format(url2.split('/')[-1]))
            try:
                r = requests.get(dat_dict2, stream=True)
                if(r.status_code == requests.codes.ok):
                    with open(out_text_name2,"wb") as fd:
                        for chunk in r.iter_content(chunk_size=1024):
                            if chunk:
                                fd.write(chunk)
                        fd.close()
            except:
                np.nan

        print("{} data has been exported to your desktop in the cpsData folder".format(year))

    for filename in glob.iglob(os.path.join(yearpath_uz, '*.cps')):
        os.rename(filename, filename[:-4] + '.dat')

def get_dict(file):
    '''
    retrieves dictionary containing commands used when developing aggregate tables
    
    :param file: string filepath to where .json dictionary file is saved
    '''
    #"C:/Users/mossg/OneDrive/Desktop/py/cps_aggregate_tables.json"
    #"C:/Users/mossg/OneDrive/Desktop/py/cps_variable_encoding.json"
    
    '''
    f = open(file,"r")
    d = json.loads(f.read())
    f.close()
    '''
    with open(file, 'rb') as f:
        d = pickle.load(f)
    
    return(d)

def clean_data(var_int, start_year_4_dig,end_year_4_dig,dfile):
    '''
    pulls in and cleans all data, combining months within a year and exporting
    them to a specific compiled data folder
    
    :param var_int: list containing variables of interest to be retained through cleaning
    :param start_year_4_dig: starting year for range observed (int)
    :param end_year_4_dig: ending year for range observed (int)
    :param dfile: string file path to nums_to_names data cleaning dictionary
    '''
    #create path for output
    outputdir = outpath + 'cps_clean_data'
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    
    for year in range(int(start_year_4_dig),int(end_year_4_dig)+1):
        #check year, define extension
        yr = int(str(year)[2:4])            
        
        inputdir = newpath + '/cps_' + str(yr)
        
        #list of months
        mo_list = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
                
        dataframe = pd.DataFrame()
        
        print("loading " + str(year) + " data [",end="")
        
        #loop through month .dat files
        for mo in range(1,len(mo_list)+1):
            try:
                #set variable name to be used to locate .dat files
                month = month_switch(mo)
                varnam = '/' + month + str(yr)
                
                # Data dictionary location of most recent data dictionary
                if year == 2020:
                    dict_loc = '/cps_20/2020_Basic_CPS_Public_Use_Record_Layout_plus_IO_Code_list.txt'
                elif year in [2015,2016]:
                    dict_loc = '/cps_16/January_2015_Record_Layout.txt'
                else:
                    dict_loc = '/cps_17/January_2017_Record_Layout.txt'
                    
                dd_file = newpath + dict_loc
                dd_full = open(dd_file, encoding='iso-8859-1').read()
                
                # Regular expression finds rows with variable location details
                p = re.compile('\n(\w+)\s+(\d+)\s+(.*?)\t+.*?(\d\d*).*?(\d\d+)')
                
                # Keep adjusted results for series of interest
                dd_sel_var = [(i[0], int(i[3])-1, int(i[4])) 
                              for i in p.findall(dd_full) if i[0] in var_int]
                
                # Convert raw data into a list of tuples
                raw_tup = [tuple(int(line[i[1]:i[2]]) for i in dd_sel_var) 
                        for line in open(inputdir + varnam +"pub.dat", 'rb')]
                
                # Convert to pandas dataframe, add variable ids as heading
                df = pd.DataFrame(raw_tup, columns=[v[0] for v in dd_sel_var])
                
                #restrict to the civilian noninstitutional labor force
                df = df[df['PRTAGE'] >= 16]
                df = df[df['PRPERTYP'] == 2] #adults only, no military
                
                #eliminate non-responses from vacant housing units
                df = df[df['HWHHWGT'] > 0]
                
                #convert weights, census implies 4 decimal places
                df['PWSSWGT']=df['PWSSWGT']/10000
                df['PWORWGT']=df['PWORWGT']/10000
                df['PWVETWGT']=df['PWVETWGT']/10000
                
                #pretty sure the avg weekly earnings variable has implied 2 decimal places
                df['PRERNWA']=df['PRERNWA']/100
                
                #add individual weight in order to calculate number of observations
                df['individual']=1;
                
                #recode full and part time employment
                df['EMP_STATUS'] = np.nan
                df.loc[(df['PREMPNOT']==1) & (df['PRFTLF']==2),'EMP_STATUS']="Part-time"
                df.loc[(df['PREMPNOT']==1) & (df['PRFTLF']==1),'EMP_STATUS']="Full-time"
                
                #age recode
                df['age']="25 to 54"
                df.loc[df['PRTAGE'] >= 55,'age'] = "55 and older"
                df.loc[(16<=df['PRTAGE']) & (df['PRTAGE']<25),'age'] = "16 to 24"
                df.loc[df['PRTAGE'] < 16,'age'] = np.nan
                
                df['age2']="65 and older"
                df.loc[(16<=df['PRTAGE']) & (df['PRTAGE']<25),'age2'] = "16 to 24"
                df.loc[(25<=df['PRTAGE']) & (df['PRTAGE']<35),'age2'] = "25 to 34"
                df.loc[(35<=df['PRTAGE']) & (df['PRTAGE']<45),'age2'] = "35 to 44"
                df.loc[(45<=df['PRTAGE']) & (df['PRTAGE']<55),'age2'] = "45 to 54"
                df.loc[(55<=df['PRTAGE']) & (df['PRTAGE']<65),'age2'] = "55 to 64"
                
                #educational attainment recode
                df.loc[(df['PRTAGE']>24) & (31<=df['PEEDUCA']) & (df['PEEDUCA']<=38),'EDUC']="NO HIGH SCHOOL DIPLOMA"
                df.loc[(df['PRTAGE']>24) & (df['PEEDUCA']==39),'EDUC']="HS GRADUATE, NO COLLEGE"
                df.loc[(df['PRTAGE']>24) & (df['PEEDUCA']==40),'EDUC']="SOME COLLEGE, NO DEGREE"
                df.loc[(df['PRTAGE']>24) & (41<=df['PEEDUCA']) & (df['PEEDUCA']<=42),'EDUC']="ASSOCIATES DEGREE"
                df.loc[(df['PRTAGE']>24) & (df['PEEDUCA']==43),'EDUC']="BACHELORS DEGREE"
                df.loc[(df['PRTAGE']>24) & (44<=df['PEEDUCA']) & (df['PEEDUCA']<=46),'EDUC']="ADVANCED DEGREE"
                
                df.loc[(df['PRTAGE']>24) & (40<=df['PEEDUCA']) & (df['PEEDUCA']<=42),'EDUC2']="SOME COLLEGE OR ASSOCIATES"
                df.loc[(df['PRTAGE']>24) & (43<=df['PEEDUCA']) & (df['PEEDUCA']<=46),'EDUC2']="BACHELORS OR HIGHER"
                
                #changed mid skilled classification for most recent update (henry's words)
                df.loc[(df['PRTAGE']>24) & (31<=df['PEEDUCA']) & (df['PEEDUCA']<=38),'EDUC3']="NO HIGH SCHOOL DIPLOMA"
                df.loc[(df['PRTAGE']>24) & (df['PEEDUCA']==39),'EDUC3']="HS GRADUATE OR GED"
                df.loc[(df['PRTAGE']>24) & (40<=df['PEEDUCA']) & (df['PEEDUCA']<=42),'EDUC3']="MID-SKILLED"
                df.loc[(df['PRTAGE']>24) & (43<=df['PEEDUCA']) & (df['PEEDUCA']<=46),'EDUC3']="BACHELORS OR HIGHER"
                
                #changed mid skilled classification for most recent update (henry's words)
                df.loc[(df['PRTAGE']>24) & (31<=df['PEEDUCA']) & (df['PEEDUCA']<=38),'EDUC3']="NO HIGH SCHOOL DIPLOMA"
                df.loc[(df['PRTAGE']>24) & (df['PEEDUCA']==39),'EDUC3']="HS GRADUATE OR GED"
                df.loc[(df['PRTAGE']>24) & (40<=df['PEEDUCA']) & (df['PEEDUCA']<=42),'EDUC3']="MID-SKILLED"
                
                #occupations recode
                df.loc[(4<=df['PRMJOCGR']) & (df['PRMJOCGR']<=5),'OCC4'] = "Farming and Construction"
                
                #race recode
                df['RACE'] = "MULTI-RACIAL"
                df.loc[df['PTDTRACE'] == 1,'RACE'] = "WHITE"
                df.loc[df['PTDTRACE'] == 2,'RACE'] = "BLACK"
                df.loc[df['PTDTRACE'] == 4,'RACE'] = "ASIAN"
                df.loc[df['PTDTRACE'] == 3,'RACE'] = "INDIGENOUS" #Native american/ Native Alaskan, may have impact on states like oklahoma
                df.loc[df['PTDTRACE'] == 5,'RACE'] = "INDIGENOUS" #Hawaiian/Pacific Islander likely to have a large impact in Hawaii, may way to break these two apart
                                
                #new variable for combined datasets
                df['HRMONTH2'] = 100*year+mo
                
                #append data
                dataframe=dataframe.append(df)
                del df
                                
                print("==", end ="")
        
            except:
                print("==", end ="")
            
        print("]")
                    
        #load data dictionary
        nums_to_names = get_dict(dfile)
    
        #load in extract files for cert and lic
        if year == 2015:
            dict_loc = '/Certification_extract_file_{}_rec_layout.txt'.format(year)
            dd_file = newpath + '/cps_' + str(yr) + dict_loc
            dd_full = open(dd_file, encoding='iso-8859-1').read()
            
            # Regular expression finds rows with variable location details
            p = re.compile('\n(\w+)\s+(\d+)\s+(.*?)\t+.*?(\d\d*).*?(\d\d+)')
            
            # Keep adjusted results for series of interest
            dd_sel_var = [(i[0], int(i[3])-1, int(i[4])) 
                          for i in p.findall(dd_full) if i[0] not in ['HRYEAR4','PXCERT1','PXCERT2']]
            
            # Convert raw data into a list of tuples
            raw_tup = [tuple(int(line[i[1]:i[2]]) for i in dd_sel_var) 
                    for line in open(inputdir + r'/jan15-dec15cert_ext.dat')]
                       
            # Convert to pandas dataframe, add variable ids as heading
            df = pd.DataFrame(raw_tup, columns=[v[0] for v in dd_sel_var])
            df.rename(columns={'MONTH':'HRMONTH'}, inplace=True)
            
            #add PECERT3 placeholder (not available in 2015)
            df['PECERT3'] = np.nan
            
            #merge with total dataframe
            dataframe = dataframe.merge(df, left_on=['QSTNUM','PULINENO','HRMONTH'], right_on=['QSTNUM','PULINENO','HRMONTH'], how='left')
        if year == 2016:
            dict_loc = '/Certification_extract_file_{}_rec_layout.txt'.format(year)
            dd_file = newpath + '/cps_' + str(yr) + dict_loc
            dd_full = open(dd_file, encoding='iso-8859-1').read()
                        
            # Regular expression finds rows with variable location details
            p = re.compile('\n(\w+)\s+(\d+)\s+(.*?)\s+.*?(\d\d*).*?(\d\d+)')
            
            # Keep adjusted results for series of interest
            dd_sel_var = [(i[0], int(i[3])-1, int(i[4])) 
                          for i in p.findall(dd_full) if i[0] not in ['HRYEAR4','PXCERT1','PXCERT2','PXCERT3']]
            
            # Convert raw data into a list of tuples
            raw_tup = [tuple(int(line[i[1]:i[2]]) for i in dd_sel_var) 
                    for line in open(inputdir + r'/jan16-dec16cert_ext.dat')]
                       
            # Convert to pandas dataframe, add variable ids as heading
            df = pd.DataFrame(raw_tup, columns=[v[0] for v in dd_sel_var])
            df.rename(columns={'MONTH':'HRMONTH'}, inplace=True)
                        
            #merge with total dataframe
            dataframe = dataframe.merge(df, left_on=['QSTNUM','PULINENO','HRMONTH'], right_on=['QSTNUM','PULINENO','HRMONTH'], how='left')
        
        dataframe.loc[dataframe['PRCIVLF'] == -1,'PRCIVLF'] = np.nan
        dataframe.loc[dataframe['PWSSWGT'] < 0,'PWSSWGT'] = np.nan
        dataframe.loc[dataframe['PECERT1'] < 0,'PECERT1'] = np.nan
        dataframe.loc[dataframe['PECERT2'] < 0,'PECERT2'] = np.nan
        
        #finish clean, recode missing values
        dataframe.replace(nums_to_names, inplace=True)

        #industry recode
        ind = {
            'Agriculture':'Agriculture and related industries',
            'Forestry, logging, fishing, and hunting':'Agriculture and related industries',
            'Mining, quarrying, and oil and gas extraction':'Mining, quarrying, and oil and gas extraction',
            'Construction':'Construction',
            'Nonmetallic mineral product manufacturing':'Manufacturing',
            'Primary metals and fabricated metal products':'Manufacturing',
            'Machinery manufacturing':'Manufacturing',
            'Computer and electronic product manufacturing':'Manufacturing',
            'Electrical equipment, appliance manufacturing':'Manufacturing',
            'Transportation equipment manufacturing':'Manufacturing',
            'Wood products':'Manufacturing',
            'Furniture and fixtures manufacturing':'Manufacturing',
            'Miscellaneous and not specified manufacturing':'Manufacturing',
            'Food manufacturing':'Manufacturing',
            'Beverage and tobacco products':'Manufacturing',
            'Textile, apparel, and leather manufacturing':'Manufacturing',
            'Paper and printing':'Manufacturing',
            'Petroleum and coal products manufacturing':'Manufacturing',
            'Chemical manufacturing':'Manufacturing',
            'Plastics and rubber products':'Manufacturing',
            'Wholesale trade':'Wholesale trade',
            'Retail trade':'Retail trade',
            'Transportation and warehousing':'Transportation and utilities',
            'Utilities':'Transportation and utilities',
            'Publishing industries (except internet)':'Information',
            'Motion picture and sound recording industries':'Information',
            'Broadcasting (except internet)':'Information',
            'Internet publishing and broadcasting':'Information',
            'Telecommunications':'Information',
            'Internet service providers and data processing services':'Information',
            'Other information services':'Information',
            'Finance':'Financial activities',
            'Insurance':'Financial activities',
            'Real estate':'Financial activities',
            'Rental and leasing services':'Financial activities',		
            'Professional, scientific, and technical services':'Professional and business services',
            'Management of companies and enterprises':'Professional and business services',
            'Administrative and support services':'Professional and business services',
            'Waste management and remediation services':'Professional and business services',
            'Educational services':'Education and health services',
            'Hospitals':'Education and health services',
            'Health care services, except hospitals':'Education and health services',
            'Social assistance services':'Education and health services',
            'Arts, entertainment, and recreation':'Leisure and hospitality',
            'Accommodation':'Leisure and hospitality',
            'Food services and drinking places':'Leisure and hospitality',
            'Repair and maintenance':'Other services',
            'Personal and laundry services':'Other services',
            'Membership associations and organizations':'Other services',
            'Private households':'Other services',
            'Public administration':'Public administration',
            'Armed forces':'Public administration'
            }
    
        #replace industry variable with more complex industry taxonomy
        dataframe['PRMJIND1'] = dataframe['PRDTIND1'].replace(ind)
        
        #add labor force participation variable
        dataframe['labforce'] = 'NOT IN LABOR FORCE'
        dataframe.loc[dataframe['PREXPLF'] == 'EMPLOYED','labforce'] = 'EMPLOYED'
        dataframe.loc[dataframe['PREXPLF'] == 'UNEMPLOYED','labforce'] = 'UNEMPLOYED'
        #dataframe.loc[dataframe['PEMLR'].isin(['NOT IN LABOR FORCE-RETIRED','NOT IN LABOR FORCE-DISABLED','NOT IN LABOR FORCE-OTHER']), 'labforce'] = 'NOT IN LABOR FORCE'

        #full time / part time lf stat
        dataframe['emp_stat'] = dataframe['labforce'].copy()
        dataframe.loc[(dataframe['emp_stat'] == 'EMPLOYED') & (dataframe['PRFTLF']=='PART TIME LABOR FORCE'),'emp_stat'] = 'PART_TIME'
        dataframe.loc[(dataframe['emp_stat'] == 'EMPLOYED') & (dataframe['PRFTLF']=='FULL TIME LABOR FORCE'),'emp_stat'] = 'FULL_TIME'

        #change non responses to nan for PECERT3
        dataframe['PECERT3'].replace({-1:np.nan},inplace=True)
       
        #export to .csv
        dataframe.to_csv(outputdir + r'/cps_' + str(yr) + '.csv', index = None, header=True)
        
        print(str(year) + " export completed")
    print("all data cleaned and compiled, stored in " + outputdir)

def create_connection(db_file):
    ''' 
    create a database connection to the SQLite database
    specified by db_file
    
    :param db_file: database file
    :return: Connection object or None
    '''
    
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
 
    return conn

def create_table(conn, create_table_sql):
    '''
    create a table from the create_table_sql statement
    
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    '''
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def build_database(database):
    '''
    constructs new database in given file path
    creates two new tables, aggregate and microdata inside db
    
    :param database: path to database ex. r"/Users/gabem/Documents/pythonsqlite.db"
    '''
    
    sql_create_microdata_table = """ CREATE TABLE IF NOT EXISTS microdata (
                                        id integer PRIMARY KEY,
                                        GESTFIPS text, 
                                        PESEX text,
                                        RACE text,
                                        PEHSPNON text,
                                        age2 text, 
                                        HRMONTH2 integer, 
                                        PWSSWGT integer,
                                        individual integer,
                                        PWORWGT integer, 
                                        PRERNWA integer, 
                                        PRTAGE integer, 
                                        EDUC text, 
                                        PEMLR text,
                                        PRCOW1 text,
                                        PRMJIND1 text,
                                        PRDTOCC1 text,
                                        PECERT1 text, 
                                        PECERT2 text,
                                        PECERT3 text,
                                        labforce text,
                                        PRERELG integer,
                                        PRCIVLF text,
                                        PWVETWGT integer,
                                        PEAFEVER integer,
                                        PEAFWHN1 integer,
                                        EDUC2 text,
                                        HRMIS integer,
                                        emp_stat text
                                    ); """
 
    sql_create_aggregate_table = """CREATE TABLE IF NOT EXISTS aggregate (
                                    id integer PRIMARY KEY,
                                    state text, 
                                    base_pop text, 
                                    labforce text,
                                    emp_type text, 
                                    emp_stat text,
                                    education text, 
                                    sex text, 
                                    race text,
                                    age text, 
                                    industry text, 
                                    occupation text,
                                    population_total integer,
                                    population_PECERT1_y integer,
                                    population_PECERT1_n integer, 
                                    population_PECERT2_y integer, 
                                    population_PECERT2_n integer,
                                    population_observed_total integer,
                                    population_observed_PECERT1_y integer, 
                                    population_observed_PECERT1_n integer,
                                    population_observed_PECERT2_y integer, 
                                    population_observed_PECERT2_n integer,
                                    median_earnings_total integer,
                                    median_earnings_PECERT1_y integer, 
                                    median_earnings_PECERT1_n integer,
                                    median_earnings_PECERT2_y integer, 
                                    median_earnings_PECERT2_n integer,
                                    earnings_observed_total integer,
                                    earnings_observed_PECERT1_y integer, 
                                    earnings_observed_PECERT1_n integer,
                                    earnings_observed_PECERT2_y integer, 
                                    earnings_observed_PECERT2_n integer
                                );"""
 
    # create a database connection
    conn = create_connection(database)
 
    # create tables
    if conn is not None:
        # create projects table
        create_table(conn, sql_create_microdata_table)
 
        # create tasks table
        create_table(conn, sql_create_aggregate_table)
    else:
        print("Error! cannot create the database connection.")

def create_aggregate(conn, agg):
    '''
    instert a new row into the aggregate table
    
    :param conn:
    :param project (tuple):
    '''
    
    sql = ''' INSERT INTO aggregate(state,base_pop,labforce,emp_type,emp_stat,education,sex,race,age,industry,occupation,population_total,population_PECERT1_y,population_PECERT1_n,population_PECERT2_y,population_PECERT2_n,population_observed_total,population_observed_PECERT1_y,population_observed_PECERT1_n,population_observed_PECERT2_y,population_observed_PECERT2_n,median_earnings_total,median_earnings_PECERT1_y,median_earnings_PECERT1_n,median_earnings_PECERT2_y,median_earnings_PECERT2_n,earnings_observed_total,earnings_observed_PECERT1_y,earnings_observed_PECERT1_n,earnings_observed_PECERT2_y,earnings_observed_PECERT2_n)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, agg)
    return cur.lastrowid

def create_microdata(conn, micro):
    """
    insert a new row into the microdata table
    
    :param conn:
    :param task (tuple):
    """
 
    sql = ''' INSERT INTO microdata(GESTFIPS,PESEX,RACE,PEHSPNON,age2,HRMONTH2,PWSSWGT,individual,PWORWGT,PRERNWA,PRTAGE,EDUC,PEMLR,PRCOW1,PRMJIND1,PRDTOCC1,PECERT1,PECERT2,PECERT3,labforce,PRERELG,PRCIVLF,PWVETWGT,PEAFEVER,PEAFWHN1,EDUC2,HRMIS,emp_stat)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, micro)
    return cur.lastrowid

def write_to_table(data,table,database):
    '''
    writes a dataframe to .db by tupleizing and parsing
    
    :param data: dataframe to be written
    :param table: destination of data (str)
    :param database: database to be written to
    '''
    # create a database connection
    conn = create_connection(database)
    with conn:
        # create a new project
        data.replace(-1, np.nan, inplace=True)
        data.replace('-1', np.nan, inplace=True)
        data.fillna('nan',inplace=True)
        
        tuples = [tuple(x) for x in data.values]

        if table == 'microdata':
            for row in range(0,len(tuples)):
                tuples[row] = str(tuples[row]).replace("d'A","dA").replace('(','').replace(')','').replace("O'B","OB").replace("y's","ys").replace("e's","es").replace(":","").replace("-"," ").replace(";","")
                micro = eval(tuples[row])
                create_microdata(conn, micro)
        else:
            for row in range(0,len(tuples)):
                tuples[row] = str(tuples[row]).replace("d'A","dA").replace('(','').replace(')','').replace("O'B","OB").replace("y's","ys").replace("e's","es").replace(":","").replace("-"," ").replace(";","")
                agg = eval(tuples[row])
                create_aggregate(conn, agg)
                
    conn.close()

def combine_data(database,start_year_4_dig,end_year_4_dig):
    '''
    combines datasets across selected years, combined years are writen to .db file
    
    :param database: path to database
    :param start_year_4_dig: starting year to be combined (int)
    :param end_year_4_dig: ending year to be combiend (int)
    '''
    outputdir = outpath + 'cps_clean_data/combined_data'
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    
    #build database and create tables
    build_database(database)
    
    inputdir = outpath + 'cps_clean_data'
        
    print("Writing microdata to database [",end="")
    
    for year in range(int(start_year_4_dig),int(end_year_4_dig)+1):

        #check year, define extension
        yr = int(str(year)[2:4])            
        
        data = inputdir + r'/cps_' + str(yr) + '.csv'
        
        df = pd.read_csv(data)
        df = df[['GESTFIPS', 'PESEX', 'RACE', 'PEHSPNON', 'age2', 'HRMONTH2', 'PWSSWGT', 
                 'individual', 'PWORWGT', 'PRERNWA', 'PRTAGE', 'EDUC', 'PEMLR', 
                 'PRCOW1', 'PRMJIND1', 'PRDTOCC1', 'PECERT1', 'PECERT2','PECERT3',
                 'labforce','PRERELG','PRCIVLF','PWVETWGT','PEAFEVER','PEAFWHN1','EDUC2','HRMIS','emp_stat']]
        
        write_to_table(df,'microdata',database)
        
        del df
        print("===", end ="")
        
    print("]", end ="")
    print('\n'+str(start_year_4_dig)+' through '+str(end_year_4_dig)+' successfully written to microdata table in '+database)

def assign_base_pop(data):
    '''
    duplicates estimates within target base population, allows us to subset for
    various population base age groups
    
    :param data: DataFrame to be extended, additional base populations added
    '''
    #copy initial data
    df = data.copy()
    #create base population column
    df.loc[25<=df['PRTAGE'],'base_pop'] = 'Civilian Population 25 and up'
    data['base_pop'] = 'Civilian Population 16 and up'

    #subset copy dataframe to 25+
    df = df[~df['base_pop'].isna()]
    
    #append to original data
    data = data.append(df)
    
    return(data)

def make_population_table(df, d):
    '''
    uses commands from dictionary object to develop aggregate tables
    
    :param df: DataFrame containing microdata
    :param d: dictionary containing aggregation commands
    '''
    
    data = df.copy()
    
    #check to see if any restrictions specified in the dict, if so, subset accordingly
    if len(d['restriction']) > 0:
        for r in d['restriction']:
            if isinstance(d['restriction'][r], list):
                for r2 in d['restriction'][r]:
                    if '!' in r2:
                        data = data[data[r] != r2.replace('!','')]
                    else:
                        data = data[data[r] == r2]
            else:            
                if '!' in d['restriction'][r] :
                    data = data[data[r] != d['restriction'][r].replace('!','')]
                else:
                    data = data[data[r] == d['restriction'][r]]

    #list of commands to perform when aggregating data
    agg_dict1 = {
        'PWSSWGT':'sum',
        'PWSSWGT_PECERT1_y':'sum',
        'PWSSWGT_PECERT1_n':'sum', 
        'PWSSWGT_PECERT2_y':'sum',
        'PWSSWGT_PECERT2_n':'sum',
        'individual':'sum',
        'individual_PECERT1_y':'sum', 
        'individual_PECERT1_n':'sum', 
        'individual_PECERT2_y':'sum',
        'individual_PECERT2_n':'sum'
        }

    agg_dict2 = {
        'PWSSWGT':'mean',
        'PWSSWGT_PECERT1_y':'mean',
        'PWSSWGT_PECERT1_n':'mean', 
        'PWSSWGT_PECERT2_y':'mean',
        'PWSSWGT_PECERT2_n':'mean',
        'individual':'sum',
        'individual_PECERT1_y':'sum', 
        'individual_PECERT1_n':'sum', 
        'individual_PECERT2_y':'sum',
        'individual_PECERT2_n':'sum'
        }

    h = {}
    
    #loop through each table in the dictionary, ignoring key and restriction fields
    #aggregate as specified in dictionary
    for grp in d:   
        if grp not in ['key','restriction']:
            
            temp = pd.DataFrame(data.groupby(d[grp]['group']+['HRMONTH2']).agg(agg_dict1))
            temp.replace(0,np.nan,inplace=True)
        
            h[grp] = pd.DataFrame(temp.groupby(d[grp]['group']).agg(agg_dict2)).reset_index()

            for item in d[grp]['fill']:
                h[grp][item] = d[grp]['fill'][item]
    
    out = pd.DataFrame()
    for dat in h:
        out = out.append(h[dat])
    
    #rename columns
    out.rename(columns={
        'PWSSWGT':'population_total',
        'PWSSWGT_PECERT1_y':'population_PECERT1_y',
        'PWSSWGT_PECERT1_n':'population_PECERT1_n', 
        'PWSSWGT_PECERT2_y':'population_PECERT2_y',
        'PWSSWGT_PECERT2_n':'population_PECERT2_n',
        'individual':'population_observed_total',
        'individual_PECERT1_y':'population_observed_PECERT1_y', 
        'individual_PECERT1_n':'population_observed_PECERT1_n', 
        'individual_PECERT2_y':'population_observed_PECERT2_y',
        'individual_PECERT2_n':'population_observed_PECERT2_n'
        }, inplace=True)
    
    return(out)

def make_earnings_table(df, d):
    '''
    uses commands from dictionary object to develop aggregate tables
    
    :param df: DataFrame containing microdata
    :param d: dictionary containing aggregation commands
    '''
    
    data = df.copy()
    
    data = data[data['PRERELG']==1] # restricts to employed, outgoing groups
    
    if len(d['restriction']) > 0:
        for r in d['restriction']:
            if isinstance(d['restriction'][r], list):
                for r2 in d['restriction'][r]:
                    if '!' in r2:
                        data = data[data[r] != r2.replace('!','')]
                    else:
                        data = data[data[r] == r2]
            else:            
                if '!' in d['restriction'][r] :
                    data = data[data[r] != d['restriction'][r].replace('!','')]
                else:
                    data = data[data[r] == d['restriction'][r]]


    obs_col = [i for i in data.columns if 'individual' in i]
    weight_col = [i for i in data.columns if 'PWORWGT' in i]

    agg_dict1 = {
        'PWORWGT':'sum',
        'PWORWGT_PECERT1_y':'sum',
        'PWORWGT_PECERT1_n':'sum', 
        'PWORWGT_PECERT2_y':'sum',
        'PWORWGT_PECERT2_n':'sum',
        'individual':'sum',
        'individual_PECERT1_y':'sum', 
        'individual_PECERT1_n':'sum', 
        'individual_PECERT2_y':'sum',
        'individual_PECERT2_n':'sum'
        }

    agg_dict2 = {
        'PWORWGT':'mean',
        'PWORWGT_PECERT1_y':'mean',
        'PWORWGT_PECERT1_n':'mean', 
        'PWORWGT_PECERT2_y':'mean',
        'PWORWGT_PECERT2_n':'mean',
        'individual':'sum',
        'individual_PECERT1_y':'sum', 
        'individual_PECERT1_n':'sum', 
        'individual_PECERT2_y':'sum',
        'individual_PECERT2_n':'sum'
        }

    h = {}
    
    for grp in d:   
        if grp not in ['key','restriction']:
            
            temp = pd.DataFrame(data.groupby(d[grp]['group']+['HRMONTH2','PRERNWA']).agg(agg_dict1))
            temp.replace(0,np.nan,inplace=True)
            
            temp2 = pd.DataFrame(temp.groupby(d[grp]['group']+['PRERNWA']).agg(agg_dict2))
            #sum total observations
            obs = temp2[obs_col].groupby(d[grp]['group'])[obs_col].agg('sum')
            
            temp2.drop(obs_col, axis=1, inplace=True)
            #determine cuttoff for each earnings column
            cutoff = temp2.groupby(d[grp]['group'])[weight_col].agg('sum') / 2.0
            #cumulatively sum down each column
            cumsum = temp2.groupby(d[grp]['group'])[weight_col].cumsum()
            #determine the boundary limit for each column
            boundary = cumsum.ge(cutoff).reset_index()
            boundary['PRERNWA'] = boundary['PRERNWA'].astype('str')
            #find the first observation in each group which surpases or equals the boundary for that group
            median = boundary.set_index('PRERNWA').groupby(d[grp]['group'])[weight_col].apply(pd.DataFrame.idxmax).astype('float64')
            #address cells with no observations (these columns will just return the lowest listed earnings amount, we want it to be null)
            nanframe = obs.replace(0,np.nan)
            nanframe = np.where((obs > 0),1,nanframe)
            
            median *= nanframe
            
            h[grp] = median.join(obs,how='outer').reset_index()

            for item in d[grp]['fill']:
                h[grp][item] = d[grp]['fill'][item]
                
    out = pd.DataFrame()
    for dat in h:
        out = out.append(h[dat])
    
    out.rename(columns={
        'PWORWGT':'median_earnings_total',
        'PWORWGT_PECERT1_y':'median_earnings_PECERT1_y',
        'PWORWGT_PECERT1_n':'median_earnings_PECERT1_n', 
        'PWORWGT_PECERT2_y':'median_earnings_PECERT2_y',
        'PWORWGT_PECERT2_n':'median_earnings_PECERT2_n',
        'individual':'earnings_observed_total',
        'individual_PECERT1_y':'earnings_observed_PECERT1_y', 
        'individual_PECERT1_n':'earnings_observed_PECERT1_n', 
        'individual_PECERT2_y':'earnings_observed_PECERT2_y',
        'individual_PECERT2_n':'earnings_observed_PECERT2_n'
        }, inplace=True)
    
    return(out)

def suppress_output(df,key,sup_val=30):
    '''
    suppresses output from aggregate tables based on 'true' observations,
    that is to say unique person identifiers, this prevents us from overcounting
    each person up to 8 times due to their inclusion in multiple months. for the
    purposes of suppression, individuals are only counted if observed in the
    4th month of the survey. this month was chosen as it is the first time an
    individual is eligible to answer questions related to earnigns - an earlier
    or later month (other than month 8) would show 0 responses for earnings
    questions.
    
    :param df: DataFrame containing aggregate tables
    :param key: list merge key from table creation dictionary
    :param sup_val: int default 30, the minimum number of observations to not be suppressed
    '''
    data = df.copy()
        
    cols = [
        'population_total',
        'population_PECERT1_y',
        'population_PECERT1_n', 
        'population_PECERT2_y', 
        'population_PECERT2_n',
        'population_observed_total',
        'population_observed_PECERT1_y', 
        'population_observed_PECERT1_n',
        'population_observed_PECERT2_y', 
        'population_observed_PECERT2_n',
        'median_earnings_total',
        'median_earnings_PECERT1_y', 
        'median_earnings_PECERT1_n',
        'median_earnings_PECERT2_y', 
        'median_earnings_PECERT2_n',
        'earnings_observed_total',
        'earnings_observed_PECERT1_y', 
        'earnings_observed_PECERT1_n',
        'earnings_observed_PECERT2_y', 
        'earnings_observed_PECERT2_n'
        ]
    #build a matching dataframe to act as a suppression lattice
    supframe = pd.DataFrame(index=range(len(data)),columns=key + cols).fillna(1)
    
    obs_cols = [i for i in cols if 'observed' in i]
    
    #loop through observation columns, if any observation is less than the threshold
    #suppress the corresponding cell in the estimate column
    for col in obs_cols:
        name = col.replace('_observed','').replace('earnings','median_earnings')
#        supframe[col] = 1
        supframe[name] = [1 if i >= sup_val else np.nan for i in data[col]]
    
    #multiply data by supframe, nullifying any cells with lacking observations
    out = data * supframe
    
    return(out)

def get_microdata(database, table='microdata'):
    '''     
    retrieves microdata from database and cleans it for use in table aggregation
    
    :param database: string file path to databse
    :param table: string default 'microdata' table name containing microdata
    '''
    #database column names
    cols = [
        'GESTFIPS',
        'labforce',
        'PRCOW1',
        'emp_stat',
        'EDUC',
        'EDUC2',
        'PESEX',
        'race',
        'age2',
        'PEHSPNON',
        'PRMJIND1',
        'PRDTOCC1',#'soc_2dig',
        'PRTAGE',
        'HRMONTH2',
        'PWSSWGT',
        'PWORWGT',
        'individual',
        'PRERNWA',
        'PRERELG',
        'PECERT1',
        'PECERT2',
        'HRMIS'
        ]
    
    #user friendly names
    names = [
        'state',
        'labforce',
        'emp_type',
        'emp_stat',
        'education',
        'EDUC2',
        'sex',
        'race',
        'age',
        'hispanic',
        'industry',
        'occupation',
        'PRTAGE',
        'HRMONTH2',
        'PWSSWGT',
        'PWORWGT',
        'individual',
        'PRERNWA',
        'PRERELG',
        'PECERT1',
        'PECERT2',
        'month'
        ]

    #read in data    
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    df = pd.DataFrame(cur.execute("SELECT {} FROM {}".format(', '.join(cols),table)).fetchall())
    df.replace({'nan':np.nan},inplace=True)    
    
    df.columns = names

    #create a base population variable for education comparisons
    df = assign_base_pop(df)

    #relable individual observations, count only those persons in month 4 of the survey for suppression
    df['individual'] = [1 if i == 4 else 0 for i in df['month']] 

    #create boolean certification variables
    df['PECERT1_y'] = [str(i).upper()=='YES' for i in df['PECERT1']]
    df['PECERT1_n'] = [str(i).upper()=='NO' for i in df['PECERT1']]
    df['PECERT2_y'] = [str(i).upper()=='YES' for i in df['PECERT2']]
    df['PECERT2_y'] = df['PECERT1_y'] & df['PECERT2_y']
    df['PECERT2_n'] = [str(i).upper()=='NO' for i in df['PECERT2']]
    df['PECERT2_n'] = df['PECERT1_y'] & df['PECERT2_n']
    
    #expand data using boolean columns
    for col in ['PWSSWGT', 'individual', 'PWORWGT']:
        df['{}_PECERT1_y'.format(col)] = df[col] * df['PECERT1_y']
        df['{}_PECERT1_n'.format(col)] = df[col] * df['PECERT1_n']
        df['{}_PECERT2_y'.format(col)] = df[col] * df['PECERT2_y']
        df['{}_PECERT2_n'.format(col)] = df[col] * df['PECERT2_n']
            
    return(df)

def assign_base_values(df,key):
    '''
    fills in missing columns in aggregate tables, assigns default values for each column
    
    :param df: DataFrame containing aggregate data
    :param key: list merge key from table creation dictionary
    '''
    data = df.copy()
    
    #dictionary containing default values for each column
    defaults = {
        'state':'US',
        'base_pop':'Civilian Population 16 and up',
        'labforce':'TOTAL POPULATION',
        'emp_type':'TOTAL POPULATION',
        'emp_stat':'TOTAL POPULATION',
        'education':'ALL EDUCATION LEVELS',
        'sex':'ALL GENDERS',
        'race':'ALL RACES',
        'age':'ALL AGES',
        'industry':'ALL INDUSTRY',
        'occupation':'ALL OCCUPATION'
        }

    categories = [i for i in defaults if i not in key]
    #assign default columns
    for c in categories:
        data[c] = defaults[c]
    #relable employment type if laborforce is employed
    if 'emp_type' in categories:
        data.loc[data['labforce'] == 'EMPLOYED','emp_type'] = 'TOTAL EMPLOYED POPULATION'

    if 'emp_stat' in categories:
        data.loc[data['emp_type'] == 'EMPLOYED','emp_stat'] = 'TOTAL EMPLOYED POPULATION'
                
    data.loc[data['labforce'] == 'UNEMPLOYED','emp_type'] = 'TOTAL UNEMPLOYED POPULATION'
    data.loc[data['labforce'] == 'NOT IN LABOR FORCE','emp_type'] = 'TOTAL PERSONS NOT IN LABOR FORCE'
    data.loc[data['labforce'] == 'IN LABOR FORCE','emp_type'] = 'TOTAL PERSONS IN LABOR FORCE'

    data.loc[data['emp_type'] == 'TOTAL UNEMPLOYED POPULATION','emp_stat'] = 'UNEMPLOYED'
    data.loc[data['emp_type'] == 'TOTAL PERSONS NOT IN LABOR FORCE','emp_stat'] = 'NOT IN LABOR FORCE'
    data.loc[data['emp_type'] == 'TOTAL PERSONS IN LABOR FORCE','emp_stat'] = 'IN LABOR FORCE'
        
    return(data)


def generate_tables(df, d, database, fname, table_out = 'aggregate'):
    '''
    uses dictionary to generate aggregate tables and write them to file
    
    :param df: DataFrame containing microdata
    :param d: dictionary containing table creation commands
    :param database: string file path to database for optional output method
    :param fname: string file name of output file
    :param table_out: string default 'aggregate', optional table name for data to be written to
    '''
    data = df.copy()    
    
    col_order = [
        'state', 
        'base_pop', 
        'labforce',
        'emp_type', 
        'emp_stat',
        'education', 
        'sex', 
        'race',
        'age', 
        'industry', 
        'occupation',
        'population_total',
        'population_PECERT1_y',
        'population_PECERT1_n', 
        'population_PECERT2_y', 
        'population_PECERT2_n',
        'population_observed_total',
        'population_observed_PECERT1_y', 
        'population_observed_PECERT1_n',
        'population_observed_PECERT2_y', 
        'population_observed_PECERT2_n',
        'median_earnings_total',
        'median_earnings_PECERT1_y', 
        'median_earnings_PECERT1_n',
        'median_earnings_PECERT2_y', 
        'median_earnings_PECERT2_n',
        'earnings_observed_total',
        'earnings_observed_PECERT1_y', 
        'earnings_observed_PECERT1_n',
        'earnings_observed_PECERT2_y', 
        'earnings_observed_PECERT2_n'
        ]
    
    earn_cols = [i for i in col_order if 'earnings' in i]

    for tab in d:
        print('current table: {}'.format(tab))
        #generate population and earnings tables
        pop = make_population_table(data,d[tab])
        
        #if labforce is specified as a group not eligible to report income, create null earnings table
        if 'labforce' in d[tab]['restriction'] and any(x in d[tab]['restriction']['labforce'] for x in ['UNEMPLOYED','NOT IN LABOR FORCE']):
            for e in earn_cols:
                pop[e] = np.nan
            out = pop.copy()
        else:
            earn = make_earnings_table(data,d[tab])
                
            out = pop.merge(earn,on=d[tab]['key'],how='outer')        
        
        #suppress values
#        out = suppress_output(out, d[tab]['key'])
        
        #relable EDUC2 variable for lumina tables
        out.rename(columns={'EDUC2':'education'},inplace=True)
        d[tab]['key'] = [i.replace('EDUC2','education') for i in d[tab]['key']]

        #assign base population values
        out = assign_base_values(out,d[tab]['key'])
        
        #properly order columns
        out = out[col_order]
        
        #write to table
        with open('{}{}.csv'.format(outpath,fname), 'a', newline='') as f:
            out.to_csv(f, mode='a', header=f.tell()==0)
        #write_to_table(out,table_out,database)
        
        print('{} written to file'.format(tab))
        
def create_aggregate_table(database,metadata,fname):
    '''
    entry point for aggregate table creation process
    
    :param database: string file path to database containing microdata
    :param metadata: string file path to dictionary containing table creation commands
    :param fname: string output table name
    '''
    
    d = get_dict(metadata)
    data = get_microdata(database)

    generate_tables(data, d, database, fname)
    
        
def smooth_data(df):
    '''
    Parameters
    ----------
    df : pd.DataFrame()
        DataFrame containing rough estimates for cert/lic that requires smoothing.

    Returns
    -------
    None.
    '''
    
    def get_shares(df,small,large):
        s_n = '{}_sh'.format(small)

        df[s_n] = df[small].fillna(0) / (df[small].fillna(0)+df[large].fillna(0))
        df[s_n].fillna(0,inplace=True)

    get_shares(df,'population_PECERT1_y','population_PECERT1_n')        
    get_shares(df,'population_PECERT2_y','population_PECERT2_n')
    
    def get_smooth(df,small,large,target): 
        s_n = '{}_sh'.format(small)
        s_sm = '{}_sm'.format(small)
        l_sm = '{}_sm'.format(large)
        
        df[s_sm] = df[target]*df[s_n]
        df[l_sm] = df[target] - df[s_sm]
        
    get_smooth(df,'population_PECERT1_y','population_PECERT1_n','population_total')
    get_smooth(df,'population_PECERT2_y','population_PECERT2_n','population_PECERT1_y_sm')
    
    df.drop(['population_PECERT1_y_sh','population_PECERT2_y_sh'],axis=1,inplace=True)

def convert(fname):
    '''
    smooths and drop duplicates from .csv output file
    
    :param fname: string file path to .csv file containing aggregate output
    '''
    df = pd.read_csv(fname)
    
    #catch index written to file
    try:
        df.drop('Unnamed: 0',axis=1,inplace=True)
    except:
        np.nan
    
    #smooth across columns
    smooth_data(df)
    
    #drop erronious rows
    df = df[~df['state'].isna()]
    
    #drop duplicates
    df.drop_duplicates(inplace=True)
    
    col_order = [
        'state', 
        'base_pop', 
        'labforce',
        'emp_type', 
        'emp_stat',
        'education', 
        'sex', 
        'race',
        'age', 
        'industry', 
        'occupation',
        'population_total',
        'population_PECERT1_y',
        'population_PECERT1_n', 
        'population_PECERT2_y', 
        'population_PECERT2_n',
        'population_observed_total',
        'population_observed_PECERT1_y', 
        'population_observed_PECERT1_n',
        'population_observed_PECERT2_y', 
        'population_observed_PECERT2_n',
        'median_earnings_total',
        'median_earnings_PECERT1_y', 
        'median_earnings_PECERT1_n',
        'median_earnings_PECERT2_y', 
        'median_earnings_PECERT2_n',
        'earnings_observed_total',
        'earnings_observed_PECERT1_y', 
        'earnings_observed_PECERT1_n',
        'earnings_observed_PECERT2_y', 
        'earnings_observed_PECERT2_n',
        'population_PECERT1_y_sm',
        'population_PECERT1_n_sm',
        'population_PECERT2_y_sm',
        'population_PECERT2_n_sm'
        ]
    
    dataframe = df.copy()
    dataframe = dataframe[col_order]

    dataframe.to_csv(fname, index=False, header=True)

def main(var_int, outpath, start_year, end_year):
    '''
    coordinates order of functions above
    :param database: path to database file (str)
    :param var_int: variables of interest to restrict dataframes in cleaning step (list[str])
    :param start_year end_year: starting and ending years to be observed (int)
    '''
    #define file path to database
    database = outpath + 'FILE PATH TO DATABASE'
    
    #metadata location, metadata is a json file containing instructions on which aggregate tables to build
    metadata = "FILE PATH TO METADATA"
    #json dictionary of cps encodings used to decode raw cps data, based on data dictionary provided by census
    variable_encoding = "FILE PATH TO CPS DICTIONARY"

    #get data and build aggregate file
    get_raw_data(start_year,end_year)
    clean_data(var_int,start_year,end_year,variable_encoding)
    combine_data(database,start_year,end_year)
    create_aggregate_table(database,metadata,'cps_aggregate_database_{}_{}'.format(start_year,end_year))
    convert('{}cps_aggregate_database_{}_{}.csv'.format(outpath,start_year,end_year))


if __name__ == '__main__':
    #variables of interest from cps to be retained when compiling and cleaning
    var_int = ['QSTNUM', 'PULINENO', 'OCCURNUM', 'HURESPLI', 'HUFINAL', 'HWHHWGT', 'HRMONTH', 'HEFAMINC', 
           'PRCIVLF', 'PREMPNOT', 'PWSSWGT', 'PEEDUCA', 'PEAFNOW', 'PEIO1COW', 'PEIO2COW', 
           'PRCOW1', 'PRCOW2', 'PRDTOCC1', 'PRDTOCC2', 'PREMP', 'PRMJIND1', 'PRMJIND2', 
           'PRMJOCC1', 'PRMJOCC2', 'PEIO1OCD', 'PEIO2ICD', 'PEIO2OCD', 'PEHRFTPT', 'PEMLR', 
           'PRTAGE', 'PRTFAGE', 'PESEX', 'PTDTRACE', 'PRDTHSP', 'PEHSPNON', 'PRERNWA', 
           'PTWK', 'GEREG', 'PECERT1', 'PECERT2', 'PECERT3', 'PREXPLF', 'PRFTLF', 'PRHRUSL', 'PRPTHRS', 
           'PRPTREA', 'PRWKSTAT', 'PRDTIND1', 'GESTFIPS', 'GTCBSA', 'GTCO', 'GTCBSAST', 
           'GTMETSTA', 'GTINDVPC', 'GTCBSASZ', 'GTCSA', 'PRMJOCGR', 'PEERNLAB', 'PRCITSHP', 
           'PRERNHLY', 'PEERNCOV', 'PWORWGT', 'HRYEAR4','PRERELG','PRPERTYP','PWVETWGT','PEAFEVER','PEAFWHN1','HRMIS']
    
    #directory to store files downloaded and produced during the creation of this data
    newpath = 'PATH TO DIRECTORY'
    if not os.path.exists(newpath):
        os.makedirs(newpath)
        
    #define path for downloaded data
    outpath = 'PATH TO STORE ZIP FILES DOWNLOADED FROM CENSUS'

    #run
    main(var_int, outpath, 2016, 2020)
    




    