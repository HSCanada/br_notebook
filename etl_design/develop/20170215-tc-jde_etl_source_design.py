
# coding: utf-8

# # JDE ETL Source Design
# ## Goal:  Generate source SQL with friendly names, mapped to IT table fields

# In[1]:

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import os, sys
import warnings

warnings.filterwarnings('ignore')


# ## Setup DB Connection and retrieve table metadata

# ### Input Parameters

# In[2]:

sql_connection_str = 'mssql+pymssql://sql2srv:Password1@CAHSIONNLSQL2.ca.hsi.local:1433/BRSales'
sql_link_server = 'ESYS_QA'
sql_lib = 'ARCPDTA93'
# ARC | P | 

# update Tablename
sql_table = 'F9860'

# F4072 F4101 F5613 F4072 F4094 F5830 F5831 F5832 F8444


# In[3]:

engine = create_engine(sql_connection_str)


# In[4]:

sql_meta = '''

SELECT * from OPENQUERY ({}, '	
	SELECT
		*
	FROM
		QSYS2.SYSCOLUMNS
	WHERE
        TABLE_SCHEMA = ''{}'' AND
		TABLE_NAME in( ''{}'')
    ORDER BY 
        ORDINAL_POSITION
')

''' .format(sql_link_server, sql_lib, sql_table)


# In[5]:

df = pd.read_sql_query(sql_meta, engine);


# ### Cleanup field information

# df.TABLE_SCHEMA.unique()
# 
# array(['HSICDCPROD', 'HSIPDTA93', 'ARCCDCPROD', 'PGMRWORK', 'JDEDTA93',
#        'ARCPDTA93', 'ARCPDTA94', 'HSIPDTA93S', 'HSIPDTA71', 'ARCPDTA71'], dtype=object)

# In[6]:

print (sql_meta)


# In[7]:

print(df.dtypes)
df


# In[8]:

df2 = df[['ORDINAL_POSITION', 'COLUMN_NAME', 'COLUMN_TEXT', 'DATA_TYPE','LENGTH', 'NUMERIC_PRECISION']]


# remove trailing dots, special characters, and converto to lower_case

# In[9]:

df2.COLUMN_TEXT = df2.COLUMN_TEXT.str.rstrip('. ').str.replace(r'[^0-9|a-z|_|" "]','', case=False).str.replace('  ',' ').str.lower().str.replace(' ','_')


# ### Create SQL select mapping

# In[10]:

sql_field_sel = ', '.join([x if y != 'DECIMAL' else 'DECIMAL({}/10000,10,4) as {}'.format(x,x) for x, y in zip(df2['COLUMN_NAME'], df2['DATA_TYPE'])])
sql_field_map = ', '.join(['"{}" AS {}'.format(x,y if not str.isdigit(y[0]) else "_" + y)  for x, y in zip(df2['COLUMN_NAME'], df2['COLUMN_TEXT'])])


# In[11]:

sql_output = '''

SELECT 

    TOP 5 {} 
    
FROM 

    OPENQUERY ({}, '

	SELECT
		{}
        
	FROM
		{}.{}
')

'''.format(sql_field_map, sql_link_server, sql_field_sel, sql_lib, sql_table)


# In[12]:

print(sql_output)


# ### Output Table

# In[13]:

df_output = pd.read_sql_query(sql_output, engine);


# In[ ]:

df_output.T


# ### Next steps...
# Add SQL to SQL Tools data package 
