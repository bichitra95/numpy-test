
# coding: utf-8

# ## Numpy-test
# Assignment for RedCarpetUp

# ### Importing necessary libraries 

# In[2]:

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import os
import re
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile


# ### Creating Data file for storing outputs

# In[3]:

file_path="data_file"
if not os.path.exists(file_path):
    os.mkdir(file_path)


# ### Defining two functions
# 
#     1.sec_dataframe: 
#         a.Input: getting the link for finding url's of zip file
#         b.Execution:finding "associated-data-ditribution" class as it contain the table of zip file. Then extracting the date and exempt information from the link itself.
#         c.Return: This function return the dataframe containing url of zip, Date and type of zip links.
#         
#         
#     2.get_sec_zip_by_period:
#         a.Input: getting periods, exempt and recent option to extract files
#         b.Execution: condition1: if opted for most recent option (True) then function will execute last executed time periods
#                      condition2: if not opted for most recent option (False) then periods passed will be executed.
#                      checking if there is any last executed file 
#         c.Output: Extracting excel files for the given periods and joining all the excel files
#                      

# In[5]:

def sec_dataframe(link) :    
    page = requests.get(link)
    soup = BeautifulSoup(page.text, 'html.parser')
    artist_name_list = soup.find(class_='associated-data-distribution')
    artist_name_list_items = artist_name_list.find_all('a')
    sec=['https://www.sec.gov'+artist_name.get('href') for artist_name in artist_name_list_items]
    sec=pd.DataFrame(data=sec,columns=['File_URL'])
    sec['File_Name']=sec['File_URL'].apply(lambda x: x.rsplit('/',1)[1])
    sec['Date']=sec['File_Name'].apply(lambda x:x[2:8])
    sec['Date']=pd.to_datetime(sec['Date'],format="%m%d%y")
    sec['type']=sec['File_Name'].apply(lambda x:'exempt' if 'exempt' in x else 'non-exempt')
    sec.drop('File_Name',axis=1,inplace=True)
    return sec


# In[8]:

def get_sec_zip_by_period(periods = [], is_exempt = False, only_most_recent=False):
    global recent_periods
    if is_exempt:
        exempt_value='exempt'
    else:
        exempt_value='non-exempt'
    if only_most_recent==True:
        try:
            recent_periods
            processing_periods=recent_periods
        except NameError:
            print ("There is no recent period processed")
            
    else:
        print("period:{}".format(periods))
        processing_periods=periods
        if len(periods)!=0:
            recent_periods=periods
            
    processing_periods=[np.datetime64(period)for period in processing_periods]
    sec_period=pd.DataFrame([],columns=sec.columns)
    for period in processing_periods:
        temp=sec[(sec['Date'].values.astype('datetime64[M]')==period) & (sec['type']==exempt_value)]
        if temp.empty:
            print('The{} period is out of table.'.format(period))
        sec_period=sec_period.append(temp)
    sec_details=pd.DataFrame()
    for i in np.arange(len(sec_period.index)):
        zipurl = sec_period.iloc[i]['File_URL']
        zipresp=urlopen(zipurl)
        zfile=ZipFile(BytesIO(zipresp.read()))
        file_name=zfile.namelist()
        excel_file=pd.read_excel(zfile.open(file_name[0]))
        excel_file=excel_file[['SEC Region','Organization CRD#','SEC#','Legal Name','Main Office State','5F(2)(c)','5A']]
        #print(excel_file.info())
        if sec_details.empty:
            sec_details=excel_file
        else:
            #excel_file=excel_file[['SEC Region','Organization CRD#','SEC#','Legal Name','Main Office State','5F(2)(c)','5A']]
            sec_details=sec_details.join(excel_file.set_index('Organization CRD#'),on=['Organization CRD#'],rsuffix='_second',how='outer')
    return sec_details


# ### Calling two functions to get dataframe of excel file

# In[9]:

#As the link provided in the assignment is redirecting to the below used link, we are using this instead of the given link.
link='https://www.sec.gov/help/foiadocsinvafoiahtm.html'
sec=sec_dataframe(link)
sec.head()
value=get_sec_zip_by_period(periods=['2017-11','2017-10','2000-09'],is_exempt=False,only_most_recent=False)


# ### Processing raw file to get required columns
#     1. Processing null values with average values 
#     1.getting average values of AUM, No of employees over given period of time.
#     

# In[11]:

value['SEC#']=value[['SEC#','SEC#_second']].apply(lambda row :row['SEC#_second'] if pd.isnull(row['SEC#']) else row['SEC#'],axis=1)
value['SEC Region']=value[['SEC Region','SEC Region_second']].apply(lambda row :row['SEC Region_second'] if pd.isnull(row['SEC Region']) else row['SEC Region'],axis=1)
value['Legal Name']=value[['Legal Name','Legal Name_second']].apply(lambda row :row['Legal Name_second'] if pd.isnull(row['Legal Name']) else row['Legal Name'],axis=1)
value['Main Office State']=value[['Main Office State','Main Office State_second']].apply(lambda row :row['Main Office State_second'] if pd.isnull(row['Main Office State']) else row['Main Office State'],axis=1)
value['5F(2)(c)']=value[['5F(2)(c)','5F(2)(c)_second']].apply(lambda row :row['5F(2)(c)_second'] if pd.isnull(row['5F(2)(c)']) else row['5F(2)(c)'],axis=1)
value['5F(2)(c)_second']=value[['5F(2)(c)','5F(2)(c)_second']].apply(lambda row :row['5F(2)(c)'] if pd.isnull(row['5F(2)(c)_second']) else row['5F(2)(c)_second'],axis=1)

value['5A']=value[['5A','5A_second']].apply(lambda row :row['5A_second'] if pd.isnull(row['5A']) else row['5A'],axis=1)
value['5A_second']=value[['5A','5A_second']].apply(lambda row :row['5A'] if pd.isnull(row['5A_second']) else row['5A_second'],axis=1)

value['AUM']=value[['5F(2)(c)','5F(2)(c)_second']].apply(lambda row : (row['5F(2)(c)']+row['5F(2)(c)_second'])/2,axis=1)
value['No_of_Employees']=value[['5A','5A_second']].apply(lambda row : (row['5A']+row['5A_second'])/2,axis=1)

final_sec=value.drop(['SEC Region_second','SEC#_second','Legal Name_second','Main Office State_second','5F(2)(c)','5F(2)(c)_second','5A','5A_second'],axis=1)


# In[12]:

#final_sec


# ### Generating Top managers details having highest AUM values and obtaining state wise distribution of these values

# In[13]:

Top_aum=final_sec.sort_values('AUM',ascending=False).head(15)
Top_aum.to_csv('data_file/Top_AUM.csv',index=False)

f = {'No_of_Employees':['sum'], 'AUM':['sum'],'Legal Name':['count']}
distribution=final_sec.groupby('Main Office State').agg(f)
distribution.columns=distribution.columns.droplevel(1)
distribution.to_csv('data_file/distribution.csv')


# In[16]:

# getting blackstone firms details
blackstone=final_sec[final_sec['Legal Name'].apply( lambda x: 'blackstone'.lower() in x.lower())]


# ### Finding  the blackstone firm in json file and getting their source id based on score value

# In[17]:

link='https://doppler.finra.org/doppler-lookup/api/v1/search/firms?hl=true&nrows=99000&query=blackstone&r=2500&wt=json'
json_file=pd.read_json(link)
list_of_json=pd.DataFrame(json_file['results'][0]['results'])['fields']
json_df=pd.DataFrame()
for i in np.arange(len(list_of_json)):
    d=list_of_json[i]
    df = pd.DataFrame.from_dict(d, orient='index')
    df=df.transpose()
    json_df=json_df.append(df)
result_json=pd.DataFrame()
for i in np.arange(len(blackstone)):
    blackstone_firm=blackstone['Legal Name'].iloc[i]
    list_value=[]
    for j in np.arange(len(json_df)):
        if (blackstone_firm == json_df['bc_firm_name'].iloc[j] and json_df['score'].iloc[j]>0.4 ):
            list_value.append(True)
        else:
            for k in np.arange(len(json_df['bc_other_names'].iloc[j])):
                count=0
                if (blackstone_firm == json_df['bc_other_names'].iloc[j][k] and json_df['score'].iloc[j]>0.4):
                    list_value.append(True)
                    count=1
                    break
            if count==0:
                list_value.append(False)
    result_json=result_json.append(json_df[list_value])
    result_json=result_json[['bc_firm_name','bc_source_id']]
    result_json.reset_index(inplace=True,drop=True)
        #print([True if (blackstone_firm in x) or (blackstone_firm in y) else False for i,x,y in json_df[['bc_firm_name','bc_other_names']]])
    
    


# ### getting the brochure urls of sortlisted blackstone firms

# In[19]:

brochure=pd.DataFrame()
for i in np.arange(len(result_json)):
    id_value=result_json['bc_source_id'].iloc[i]
    link='https://adviserinfo.sec.gov/IAPD/IAPDFirmSummary.aspx?ORG_PK='+id_value
    page = requests.get(link)
    soup = BeautifulSoup(page.text, 'html.parser')
    artist_name = soup.find(id='ctl00_cphMain_landing_p2BrochureLink')
    temp=['https://adviserinfo.sec.gov'+artist_name.get('href')]
    temp=pd.DataFrame(data=temp,columns=['File_URL'])
    brochure=brochure.append(temp)
brochure.reset_index(inplace=True,drop=True)
brochure=result_json.join(brochure)


# ## Downloading pdf of brochures

# In[21]:

for i in np.arange(len(brochure)):
    url=brochure['File_URL'][i]
    response = requests.get(url)
    d = response.headers['content-disposition']
    fname = re.findall("filename=(.+)", d)
    with open('data_file/'+fname[0], 'wb') as f:
        f.write(response.content)


# In[22]:

#json_df.sort_values('score',ascending=False)


# In[31]:

#search firm name in given link but the score values are very very less than 4

#json_df=pd.DataFrame()
#for i in np.arange(len(blackstone)):
#    firm=blackstone['Legal Name'].iloc[i].replace(' ','%20')
#   link='https://doppler.finra.org/doppler-lookup/api/v1/search/firms?hl=true&nrows=99000&query='+firm+'&r=2500&wt=json'
#    json_file=pd.read_json(link)
#    list_of_json=pd.DataFrame(json_file['results'][0]['results'])['fields']
    
    
#    for j in np.arange(len(list_of_json)):
#        d=list_of_json[j]
#        df = pd.DataFrame.from_dict(d, orient='index')
#        df=df.transpose()
#        json_df=json_df.append(df)
        

#'BLACKSTONE ISG-I ADVISORS L.L.C.'.replace(' ','%20')


# In[32]:

#json_df.sort_values('score',ascending=False)


# In[ ]:



