from fontTools.subset import subset

from get_catalog import get_catalog
import pandas as pd
from pyiceberg.expressions import *


# Formatting
pd.options.display.max_rows = 50
pd.options.display.max_columns = 50
pd.options.display.width = 1000
pd.options.display.colheader_justify = 'center'
pd.options.display.precision = 4
pd.options.display.float_format = '{:,.2f}'.format

# Get catalog
catalog = get_catalog()

# List tables in docs namespace
catalog.list_tables("docs")

# Load tables
company = catalog.load_table("docs.company")
ratios_ttm = catalog.load_table("docs.ratios_ttm")
forecasts = catalog.load_table("docs.forecasts")

# Apply filters, convert dates, and convert to pandas
company_df = company.scan(row_filter=(EqualTo('Sector', 'Technology'))).to_pandas()
# Pandas filter
company_df = company_df[company_df['Country'] != 'CN']
company_df['AsOfDate'] = pd.to_datetime(company_df['AsOfDate'], format='%Y-%m-%d').dt.date

ratios_ttm_df = ratios_ttm.scan(row_filter=GreaterThan('Cash_Per_Share', 0.01)).to_pandas()
ratios_ttm_df['AsOfDate'] = pd.to_datetime(ratios_ttm_df['AsOfDate'], format='%Y-%m-%d').dt.date

forecasts_df = forecasts.scan(row_filter=GreaterThanOrEqual('Dividend_Per_Share_Growth', 0.01)).to_pandas()
forecasts_df['AsOfDate'] = pd.to_datetime(forecasts_df['AsOfDate'], format='%Y-%m-%d').dt.date

# Set up some named aggregates
company_ticker_agg = company_df.groupby(group_keys=('Ticker','CalendarYear'), by='Ticker', level=0)
ratios_ttm_agg = ratios_ttm_df.groupby(group_keys=('Ticker','CalendarYear'), by='Ticker', level=0)
forecasts_agg = forecasts_df.groupby(group_keys=('Ticker', 'CalendarYear'), by='Ticker', level=0)

# Merge tables and clean up duplicates
company_ratios_df = pd.merge(company_df, ratios_ttm_df, how='inner', on='Ticker').drop_duplicates(subset=['Ticker'])
company_forecast_df = pd.merge(company_df, forecasts_df, how='inner', on='Ticker').drop_duplicates(subset=['Ticker'])

# Print results
print(company_forecast_df.head(5))
print(company_ratios_df.head(5))
print(company_ticker_agg.head(5))
# print(ratios_ttm_agg.head())
# print(forecasts_agg.head())

