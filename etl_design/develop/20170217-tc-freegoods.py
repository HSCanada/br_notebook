
# coding: utf-8

# # Investigate Free Goods in JDE

# In[59]:

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import os, sys
import warnings

warnings.filterwarnings('ignore')


# In[60]:

sql_connection_str = 'mssql+pymssql://sql2srv:Password1@CAHSIONNLSQL2.ca.hsi.local:1433/BRSales'


# In[61]:

engine = create_engine(sql_connection_str)


# In[72]:

sql = '''
SELECT 

    "ADAST" AS adjustment_name, "ADITM" AS item_number_short, "ADLITM" AS _2nd_item_number, "ADAITM" AS _3rd_item_number, "ADAN8" AS address_number, "ADICID" AS itemcustomer_key_id, "ADSDGR" AS order_detail_group, "ADSDV1" AS sales_detail_value_01, "ADSDV2" AS sales_detail_value_02, "ADSDV3" AS sales_detail_value_03, "ADCRCD" AS currency_code, "ADUOM" AS um, "ADMNQ" AS quantity_from, "ADEFTJ" AS effective_date, "ADEXDJ" AS expired_date, "ADBSCD" AS basis, "ADLEDG" AS cost_method, "ADFRMN" AS formula_name, "ADFVTR" AS factor_value, "ADFGY" AS free_goods_yn, "ADATID" AS price_adjustment_key_id, "ADURCD" AS user_reserved_code, "ADURDT" AS user_reserved_date, "ADURAT" AS user_reserved_amount, "ADURAB" AS user_reserved_number, "ADURRF" AS user_reserved_reference, "ADUSER" AS user_id, "ADPID" AS program_id, "ADJOBN" AS work_station_id, "ADUPMJ" AS date_updated, "ADTDAY" AS time_of_day 
    
FROM 

    OPENQUERY (ESYS_PROD, '

	SELECT
		ADAST, ADITM, ADLITM, ADAITM, ADAN8, ADICID, ADSDGR, ADSDV1, ADSDV2, ADSDV3, ADCRCD, ADUOM, DECIMAL(ADMNQ/10000,10,4) as ADMNQ, ADEFTJ, ADEXDJ, ADBSCD, ADLEDG, ADFRMN, DECIMAL(ADFVTR/10000,10,4) as ADFVTR, ADFGY, ADATID, ADURCD, ADURDT, DECIMAL(ADURAT/10000,10,4) as ADURAT, ADURAB, ADURRF, ADUSER, ADPID, ADJOBN, ADUPMJ, DECIMAL(ADTDAY/10000,10,4) as ADTDAY
        
	FROM
		ARCPDTA71.F4072
    WHERE
        ADAST = ''PRLINFG'' AND
        1=1
')

'''


# In[73]:

df = pd.read_sql_query(sql, engine);


# In[77]:

df.head().T


# In[79]:

df.describe()


# In[ ]:



