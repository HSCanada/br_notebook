
# coding: utf-8

# # JDE ETL Source Design
# ## Goal:  Generate source SQL with friendly names and built-in data Conversion
# 1. Pull *ALL* Field metadata based on QA 9.3:  Name, Datatype, Decimals
# 2. Pull *Specific* Table fields
# 3. Create SQL mapiing pull with data-conversion

# In[254]:

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import os, sys
import warnings

warnings.filterwarnings('ignore')


# ### Connect to SQL DB

# In[255]:

sql_connection_str = 'mssql+pymssql://sql2srv:Password1@CAHSIONNLSQL2.ca.hsi.local:1433/BRSales'
engine = create_engine(sql_connection_str)


# ### 1. Pull *ALL* Field metadata based on QA 9.3:  Name, Datatype, Decimals

# In[256]:

sql_field_meta_server = 'ESYS_PROD'
sql_field_meta_lib = 'ARCPCOM93'
#sql_field_meta_lib = 'ARCPCOM71'
#sql_field_meta_lib = 'HSIPCOM93Q'


# In[257]:

sql_field_meta = '''
SELECT 
	RTRIM("FRDTAI")				AS data_item
	,"FRDTAT"					AS data_item_type
	,"FROWTP"					AS data_type
	,"FRDTAS"					AS data_item_size
	,ISNULL("FRCDEC", 0)		AS display_decimals
	,ISNULL("FRDSCR", 'zNA')	AS row_description 
    
FROM 

    OPENQUERY ({}, '

	SELECT
		t.FRDTAI
		,FRDTAT
		,FROWTP
		,FRDTAS
		,FRCDEC
		,FRDSCR
	FROM
		{}.F9210 t
		LEFT JOIN {}.F9202 d
		ON t.FRDTAI = d.FRDTAI AND
			d.FRLNGP = '' '' AND
			d.FRSYR = '' ''  
')

'''.format(sql_field_meta_server, sql_field_meta_lib, sql_field_meta_lib)


# In[258]:

print(sql_field_meta)


# In[259]:

df_field_meta = pd.read_sql_query(sql_field_meta, engine);


# In[260]:

df_field_meta.iloc[:,[2,3,4]] = df_field_meta.iloc[:,[2,3,4]].apply(lambda x: pd.to_numeric(x, errors='coerce'))
df_field_meta.fillna(value=0,inplace=True)


# ### 2. Pull *Specific* Table fields

# #  Set Table Name HERE

# In[261]:

sql_table = 'F555116'

sql_link_server = 'ESYS_PROD'
sql_lib = 'ARCPDTA71'
#sql_lib = 'HSIPDTA71'


stage_db_schema = 'etl.'
convert_julian_date = True

# [ARC | HSI] [P | D] [DTA | CDC] [ 71 | 94]


# In[262]:

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


# In[263]:

#print (sql_table_fields)


# In[264]:

df_table_fields = pd.read_sql_query(sql_table_fields, engine);


# #### Join table fields with data dictionary meta-data

# In[265]:

#df_table_fields


# In[266]:

df_table_fields = df_table_fields[['ORDINAL_POSITION', 'COLUMN_NAME', 'COLUMN_TEXT', 'DATA_TYPE','LENGTH', 'NUMERIC_PRECISION']]


# In[267]:

df_table_fields['data_item'] = df_table_fields.COLUMN_NAME.str[2:]


# In[268]:

df_table_fields.head()


# In[269]:

df_table_fields


# In[270]:

df_table_fields.data_item.unique()


# In[271]:

dff = pd.merge(df_table_fields,df_field_meta,on='data_item', how='left')
#dff = pd.merge(df_table_fields,df_field_meta,on='data_item', how='inner')


# In[272]:

# hack to set resonable defaults for missing data
dff['display_decimals'].fillna(value='0',inplace=True)
dff['data_type'].fillna(value='0',inplace=True)
dff['row_description'].fillna(value=dff['COLUMN_TEXT'], inplace=True)


# #### Cleanup final field name
# remove trailing dots, special characters, and converto to lower_case

# In[273]:

dff['row_description_final'] = dff.row_description.str.rstrip('. ').str.replace(r'%','pct').str.replace(r'$','amt').str.replace(r'[^0-9|a-z|" "]','', case=False).str.replace('  ',' ').str.lower().str.replace(' ','_')


# #### Override with Defaults

# In[274]:

dff['row_description_final'][dff['data_item']=='LITM'] = 'item_number'
dff['row_description_final'][dff['data_item']=='AN8'] = 'billto'
dff['row_description_final'][dff['data_item']=='SHAN'] = 'shipto'
dff['row_description_final'][dff['data_item']=='DOCO'] = 'salesorder_number'


# ### 3. Create SQL mapping pull with data-conversion

# In[275]:

dff.groupby(['DATA_TYPE', 'data_type','display_decimals'])['ORDINAL_POSITION'].count()


# In[276]:

pd.options.display.max_rows = 99


# In[277]:

dff


# In[278]:

def field_format_sel(col_name, col_type, col_dec):
    val =''

    if col_type == 9 :
        if col_dec > 0 :
            val = 'CAST(({})/{} AS DEC({},{})) AS {}'.format(col_name,10**col_dec,15,np.int(col_dec),col_name)
        else :
            val = col_name
    elif  col_type == 11 : 
        if convert_julian_date :
            val = 'CASE WHEN {} IS NOT NULL THEN DATE(DIGITS(DEC({}+ 1900000,7,0))) ELSE NULL END AS {}'.format(col_name, col_name, col_name)
        else :
            val = '{} as {}'.format(col_name,col_name)
    else : 
        val = col_name
    return val;

def field_format_map(col_name, col_descr, is_etl = False):
    val =''

    col_name_format = '{message:{fill}{align}{width}}'.format(message=col_name, fill='_', align='<', width=6)
    
    col_descr_format = col_descr
    if str.isdigit(col_descr_format[0]) :
        col_descr_format = "_" + col_descr_format
        
    if is_etl :
        val = '{}_{} AS {}'.format(col_name_format, col_descr_format, col_descr_format)
    else :
        val = '"{}" AS {}_{}'.format(col_name, col_name_format, col_descr_format)

    return val;



sql_field_map = ', '.join([field_format_map(x,y)                             for x, y in zip(dff['COLUMN_NAME'], dff['row_description_final'])])

sql_field_etl = ', '.join([field_format_map(x,y,is_etl=True)                             for x, y in zip(dff['COLUMN_NAME'], dff['row_description_final'])])
#
# ok
sql_field_sel = ', '.join([ field_format_sel(col_name, col_type, col_dec)                            for col_name, col_type, col_dec in zip(dff['COLUMN_NAME'], dff['data_type'], dff['display_decimals'])])


# In[279]:

sql_field_sel


# In[280]:

sql_table_map = '''

--------------------------------------------------------------------------------
-- DROP TABLE STAGE_JDE_{}_{}_<instert_friendly_name_here>
--------------------------------------------------------------------------------

SELECT 

    {} 
    {} 

-- INTO {}{}_{}_<instert_friendly_name_here>

FROM 
    OPENQUERY ({}, '

	SELECT
		{}

	FROM
		{}.{}
--    WHERE
--        <insert custom code here>
--    ORDER BY
--        <insert custom code here>
')

--------------------------------------------------------------------------------

-- SELECT {} FROM <...>

'''.format(sql_lib, sql_table, 'Top 5', sql_field_map, stage_db_schema, sql_lib, sql_table, sql_link_server, sql_field_sel, sql_lib, sql_table, sql_field_etl)



# ### Output Table
# 1. Use to create STAGE via link and extract via IBM DTF 
# 1. Stub out missing fields from 7.1 -> 9.4, where and Autonum ID
# 1. Note that some Julian _JDT conversion will need to be converted Post 

# In[281]:

print(sql_table_map)


# In[282]:

dff


# In[283]:

get_ipython().magic('time df_table_map = pd.read_sql_query(sql_table_map, engine);')


# ### Next steps...
# Add SQL to SQL Tools data package 

# In[284]:

df_table_map


# In[ ]:




# In[ ]:



