
# coding: utf-8

# # Discounting Trends

# In[1]:

get_ipython().magic('matplotlib inline')
get_ipython().magic("config InlineBackend.figure_format='retina'")
# %load_ext autoreload
# the "1" means: always reload modules marked with "%aimport"
# %autoreload 1

from __future__ import absolute_import, division, print_function
import matplotlib as mpl
from matplotlib import pyplot as plt
from matplotlib.pyplot import GridSpec
import seaborn as sns
import mpld3
import numpy as np
import pandas as pd
import os, sys
import warnings
from sqlalchemy import create_engine
from pivottablejs import pivot_ui


# In[2]:

sns.set();
plt.rcParams['figure.figsize'] = (12, 8)
sns.set_context("poster", font_scale=1.3)
sns.set_style("whitegrid")
warnings.filterwarnings('ignore')


# In[3]:

import qgrid # Put imports at the top
qgrid.nbinstall(overwrite=True)


# ## Load Data

# In[4]:

engine = create_engine('mssql+pymssql://sql2srv:Password1@CAHSIONNLSQL2.ca.hsi.local:1433/BRSales')


# In[5]:

sql = '''
SELECT        

	t.FiscalMonth, 
	CAST (m.YearNum AS CHAR(4)) +'Q'+ CAST(m.QuarterNum AS CHAR(1)) AS year_qtr, 
	m.YearNum,  
	f.Branch, 
	i.SalesCategory, 
	t.OrderSourceCode, 
	t.PriceMethod, 
	c.SalesDivision, 
	c.SegCd, 
	SUM(t.ExtBase) AS ExtBase, 
	SUM(t.SalesAmt) AS SalesAmt, 
	SUM(t.ExtDiscLine) AS ExtDiscLine, 
	SUM(t.ExtDiscOrder) AS ExtDiscOrder, 
	SUM(t.ExtDiscAmt) AS ExtDiscAmt, 
	SUM(t.GPAmt) AS GPAmt

FROM            
	BRS_AGG_CMI_DW_Sales AS t 

	INNER JOIN BRS_FiscalMonth AS m 
	ON t.FiscalMonth = m.FiscalMonth 

	INNER JOIN BRS_Customer AS c 
	ON t.Shipto = c.ShipTo 

	INNER JOIN BRS_FSC_Rollup AS f 
	ON f.TerritoryCd = c.TerritoryCd

	INNER JOIN BRS_Item AS i 
	ON t.Item = i.Item 
	

WHERE         
	(t.SalesCategory = 'MERCH') AND 
	(t.FreeGoodsInvoicedInd = 0)  And 
--	(c.BillTo = 2613256) AND
    (f.Branch not in ('CORP', 'ZCORP')) AND
	(t.FiscalMonth BETWEEN 
		(Select [YearFirstFiscalMonth_HIST] FROM BRS_Rollup_Support01 ) and 
		(Select [PriorFiscalMonth] FROM BRS_Rollup_Support01 )
	)

GROUP BY 
	t.FiscalMonth, 
	m.YearNum, 
	m.QuarterNum, 
	f.Branch, 
	i.SalesCategory, 
	t.OrderSourceCode, 
	t.PriceMethod, 
	c.SalesDivision, 
	c.SegCd

'''


# In[6]:

df = pd.read_sql_query(sql, engine);


# In[7]:

print (df.dtypes)
df.describe()


# In[8]:

qgrid.show_grid(df, remote_js=True)


# ## Trends

# In[9]:

df.YearNum.unique()


# In[10]:

sns.barplot(data=df,x='SalesAmt',y='Branch',orient='h')


# In[11]:

get_ipython().magic("time df2 = pd.read_sql_query('select * from BRS_Item', engine);")


# In[13]:

df2.shape


# In[ ]:




# In[ ]:



