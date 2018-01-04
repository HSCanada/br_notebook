
# coding: utf-8

# # JDE ETL Source Design
# ## Goal:  Generate source SQL with friendly names and built-in data Conversion
# 1. Pull *ALL* Field metadata based on QA 9.3:  Name, Datatype, Decimals
# 2. Pull *Specific* Table fields
# 3. Create SQL mapiing pull with data-conversion

# In[171]:

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import os, sys
import warnings

warnings.filterwarnings('ignore')


# In[172]:

import qgrid # Best practices is to put imports at the top of the Notebook.
qgrid.nbinstall(overwrite=True)


# ### Connect to SQL DB

# In[173]:

sql_connection_str = 'mssql+pymssql://sql2srv:Password1@CAHSIONNLSQL2.ca.hsi.local:1433/BRSales'
engine = create_engine(sql_connection_str)


# ## 1. Pull *ALL* Field metadata based on QA 9.3:  Name, Datatype, Decimals

# In[174]:

sql_field_meta = '''
SELECT 
	RTRIM("FRDTAI")				AS data_item
	,"FRDTAT"					AS data_item_type
	,"FROWTP"					AS data_type
	,"FRDTAS"					AS data_item_size
	,ISNULL("FRCDEC", 0)		AS display_decimals
	,ISNULL("FRDSCR", 'zNA')	AS row_description 
    
FROM 

    OPENQUERY (ESYS_QA, '

	SELECT
		t.FRDTAI
		,FRDTAT
		,FROWTP
		,FRDTAS
		,FRCDEC
		,FRDSCR
	FROM
		ARCPCOM93.F9210 t
		LEFT JOIN ARCPCOM93.F9202 d
		ON t.FRDTAI = d.FRDTAI AND
			d.FRLNGP = '' '' AND
			d.FRSYR = '' ''  
')

'''


# In[175]:

df_field_meta = pd.read_sql_query(sql_field_meta, engine);


# In[176]:

df_field_meta.iloc[:,[2,3,4]] = df_field_meta.iloc[:,[2,3,4]].apply(lambda x: pd.to_numeric(x, errors='coerce'))
df_field_meta.fillna(value=0,inplace=True)


# In[178]:

print(df_field_meta.dtypes)
df_field_meta.head().T


# In[179]:

# qgrid.show_grid(df_field_meta.iloc[:,:], remote_js=True)


# ### 2. Pull *Specific* Table fields

# ####  Set Table Name HERE

# In[180]:

sql_table = 'F4072' 

sql_link_server = 'ESYS_PROD'
#sql_link_server = 'ESYS_QA'
sql_lib = 'ARCPDTA71'
#sql_lib = 'ARCPDTA93'

# F4072 F4101 F5613 F4072 F4094 F5830 F5831 F5832 F8444 F4211, F5503 

# array(['ARCPDTA93', 'ARCPCOM93', 'ARCPDTA71'], dtype=object)
# [ARC | HSI] [P | D] [DTA | CDC] [ 71 | 94]


# In[181]:

sql_table_fields = '''

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


# In[182]:

df_table_fields = pd.read_sql_query(sql_table_fields, engine);


# In[183]:

df_table_fields.head()


# #### Join table fields with data dictionary meta-data

# In[184]:

df_table_fields = df_table_fields[['ORDINAL_POSITION', 'COLUMN_NAME', 'COLUMN_TEXT', 'DATA_TYPE','LENGTH', 'NUMERIC_PRECISION']]


# In[185]:

df_table_fields['data_item'] = df_table_fields.COLUMN_NAME.str[2:]


# In[186]:

df_table_fields.data_item.unique()


# In[187]:

dff = pd.merge(df_table_fields,df_field_meta,on='data_item')


# #### Cleanup final field name
# remove trailing dots, special characters, and converto to lower_case

# In[199]:

dff['row_description_final'] = dff.row_description.str.rstrip('. ').str.replace(r'%','pct').str.replace(r'$','amt').str.replace(r'[^0-9|a-z|" "]','', case=False).str.replace('  ',' ').str.lower().str.replace(' ','_')


# #### Override with Defaults

# In[201]:

dff['row_description_final'][dff['data_item']=='LITM'] = 'item_number'
dff['row_description_final'][dff['data_item']=='AN8'] = 'billto'
dff['row_description_final'][dff['data_item']=='SHAN'] = 'shipto'
dff['row_description_final'][dff['data_item']=='DOCO'] = 'salesorder_number'


# In[202]:

print(dff.dtypes)
dff.head().T


# ### 3. Create SQL mapping pull with data-conversion

# In[203]:

dff.groupby(['DATA_TYPE', 'data_type','display_decimals'])['ORDINAL_POSITION'].count()


# In[204]:

#qgrid.show_grid(dff.iloc[:,:], remote_js=True)


# In[205]:

def field_format(col_name, col_type, col_dec):
    val =''
    if col_type == 9 :
        if col_dec > 0 :
            val = 'CAST(({})/{} AS DEC({},{})) AS {}'.format(col_name,10**col_dec,15,np.int(col_dec),col_name)
        else :
            val = col_name
    elif  col_type == 11 : 
        val = 'DATE(DIGITS(DEC( NULLIF({}, ''0001-01-01'') + 1900000,7,0))) AS {}'.format(col_name, col_name)
    else : 
        val = col_name
    return val;

sql_field_map = ', '.join(['"{}" AS {}'.format(x,y if not str.isdigit(y[0]) else "_" + y)                             for x, y in zip(dff['COLUMN_NAME'], dff['row_description_final'])])


sql_field_sel = ', '.join([ field_format(col_name, col_type, col_dec)                            for col_name, col_type, col_dec in zip(dff['COLUMN_NAME'], dff['data_type'], dff['display_decimals'])])


# In[206]:

# sql_field_sel


# In[207]:

sql_table_map = '''

SELECT 

    {} {} 
    
FROM 
    OPENQUERY ({}, '

	SELECT
		{}
        
	FROM
		{}.{}
')

'''.format('Top 5', sql_field_map, sql_link_server, sql_field_sel, sql_lib, sql_table)


# In[208]:

print(sql_table_map)


# ### Output Table

# In[209]:

get_ipython().magic('time df_table_map = pd.read_sql_query(sql_table_map, engine);')


# In[210]:

df_table_map.dtypes


# In[211]:

df_table_map


# ### Next steps...
# Add SQL to SQL Tools data package 

# In[ ]:




# In[ ]:



