from get_catalog import get_catalog
import pandas as pd
import numpy as np


catalog = get_catalog()
pd.options.display.max_rows = 50
pd.options.display.max_columns = 50
pd.options.display.width = 1000
pd.options.display.float_format = '{:,.2f}'.format

catalog.list_tables("docs")
company = catalog.load_table("docs.company")
arrow_schema = company.schema().as_arrow()
company_df = company.scan().to_pandas()

ticker_count = pd.NamedAgg(column='Ticker', aggfunc='count')
profit_growth = pd.NamedAgg(column='Profit_Growth', aggfunc=np.mean)
div_per_share = pd.NamedAgg(column='Dividend_Per_Share_Growth', aggfunc='mean')

ticker_agg = company_df.groupby('Ticker')

print(company_df.head)
