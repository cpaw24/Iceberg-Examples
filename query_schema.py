from get_catalog import get_catalog
import pandas as pd
from pyiceberg.expressions import GreaterThan, GreaterThanOrEqual, EqualTo, NotEqualTo
import matplotlib.pyplot as plt
import seaborn as sns


# Pandas output display formatting
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
company_tech = company.scan(row_filter=(EqualTo('Sector', 'Technology')))
company_tech_usd = company_tech.filter(EqualTo('Currency','USD'))
company_tech_use_df = company_tech_usd.to_pandas()
company_df = company.scan().to_pandas()

# Pandas filters; set date datatypes
company_df = company_df[company_df['Currency'] == 'USD']
company_df['AsOfDate'] = pd.to_datetime(company_df['AsOfDate'], format='%Y-%m-%d').dt.date

ratios_ttm_df = ratios_ttm.scan(row_filter=GreaterThan('Cash_Per_Share', 0.01)).to_pandas()
ratios_ttm_df['AsOfDate'] = pd.to_datetime(ratios_ttm_df['AsOfDate'], format='%Y-%m-%d').dt.date

forecasts_df = forecasts.scan(row_filter=GreaterThanOrEqual('Dividend_Per_Share_Growth', 0.000)).to_pandas()
forecasts_df['AsOfDate'] = pd.to_datetime(forecasts_df['AsOfDate'], format='%Y-%m-%d').dt.date

# Merge tables and clean up duplicates
company_ratios_df = pd.merge(company_df, ratios_ttm_df, how='inner', on='Ticker').drop_duplicates(subset=['Ticker']).reset_index(drop=True)
company_forecast_df = pd.merge(company_df, forecasts_df, how='inner', on='Ticker').drop_duplicates(subset=['Ticker']).reset_index(drop=True)
company_ratios_forecast_df = pd.merge(company_ratios_df, forecasts_df, how='inner', on='Ticker').drop_duplicates(subset=['Ticker']).reset_index(drop=True)

# company + ratios + forecasts type changes
company_ratios_forecast_df['Cash_Per_Share'] = company_ratios_forecast_df['Cash_Per_Share_x'].astype(float)
company_ratios_forecast_df['Tangible_Book_Value_Share'] = company_ratios_forecast_df['Dividend_Per_Share'].astype(float)
company_ratios_forecast_df['EBITDA_Growth_PCT'] = company_ratios_forecast_df['Tangible_Book_Value_Per_Share'].astype(float)
company_ratios_forecast_df['Free_Cash_Flow_Per_Share'] = company_ratios_forecast_df['Free_Cash_Flow_Per_Share'].astype(float)
company_ratios_forecast_df['Discounted_Cash_Flow'] = company_ratios_forecast_df["Discounted_Cash_Flow"].astype(float)
company_ratios_forecast_df['Free_Cash_Flow_Yield_PCT'] = company_ratios_forecast_df["Free_Cash_Flow_Yield_PCT"].astype(float)
company_ratios_forecast_df['Price_To_Earnings'] = company_ratios_forecast_df["Price_To_Earnings"].astype(float)
company_ratios_forecast_df['Return_On_Equity_PCT'] = company_ratios_forecast_df["Return_On_Equity_PCT"].astype(float)

# company + ratios + forecast named aggregates
company_cash_agg = pd.NamedAgg(column='Cash_Per_Share', aggfunc='mean')
company_tangible_agg = pd.NamedAgg(column='Tangible_Book_Value_Per_Share', aggfunc='median')
company_free_cash_agg = pd.NamedAgg(column='Free_Cash_Flow_Per_Share', aggfunc='mean')
company_discounted_agg = pd.NamedAgg(column='Discounted_Cash_Flow', aggfunc='mean')
company_yield_agg = pd.NamedAgg(column='Free_Cash_Flow_Yield_PCT', aggfunc='mean')
company_pe_agg = pd.NamedAgg(column='Price_To_Earnings', aggfunc='median')
company_roe_agg = pd.NamedAgg(column='Return_On_Equity_PCT', aggfunc='median')

company_cash_all = company_ratios_forecast_df.groupby(['Ticker', 'CompanyName']).agg(mean_5yr_cash=company_cash_agg,
                                                                    mean_5yr_free_cash=company_free_cash_agg,
                                                                    mean_5yr_discounted_cash=company_discounted_agg,
                                                                    mean_5yr_cash_yield=company_yield_agg)

company_metrics = company_ratios_forecast_df.groupby(['Ticker', 'CompanyName']).agg(median_5yr_tangible_bv_share=company_tangible_agg,
                                                                   median_5yr_pe=company_pe_agg,
                                                                   median_5yr_roe_pct=company_roe_agg)

# Just company + forecasts
company_forecast_df['Dividend_Per_Share_Growth'] = company_forecast_df['Dividend_Per_Share_Growth'].astype(float)
company_forecast_df['Debt_Growth_PCT'] = company_forecast_df['Debt_Growth_PCT'].astype(float)

forecast_div_agg = pd.NamedAgg(column='Dividend_Per_Share_Growth', aggfunc='mean')
forecast_div2_agg = pd.NamedAgg(column='Debt_Growth_PCT', aggfunc='median')
forecasts_metrics = forecasts_df.groupby(['Ticker']).agg(mean_5yr_dps_growth=forecast_div_agg,
                                                         median_5yr_debt_growth_pct=forecast_div2_agg)

df_div = forecasts_df.groupby('CalendarYear')['Dividend_Per_Share_Growth'].agg(['mean', 'median', 'quantile'])
df_sga = forecasts_df.groupby('CalendarYear')['Debt_Growth_PCT'].agg(['mean', 'median', 'quantile'])

# Set the style for better-looking graphs
plt.style.reload_library()
# print(plt.style.available) --  this will give you a list of style options
plt.style.use('seaborn-v0_8')

# 1. Dividend Growth and Debt Growth Over Time
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

# Dividend Growth Plot
df_div.plot(ax=ax1, marker='o')
ax1.set_title('Dividend Per Share Growth Over Time')
ax1.set_xlabel('Calendar Year')
ax1.set_ylabel('Growth Rate')
ax1.legend(['Mean', 'Median', 'Quantile'])
ax1.grid(True)

# Debt Growth Plot
df_sga.plot(ax=ax2, marker='o')
ax2.set_title('Debt Growth Percentage Over Time')
ax2.set_xlabel('Calendar Year')
ax2.set_ylabel('Growth Rate (%)')
ax2.legend(['Mean', 'Median', 'Quantile'])
ax2.grid(True)

plt.tight_layout()
plt.show()

# 2. Company Metrics Distribution
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))

# Cash Metrics
sns.boxplot(data=company_cash_all, ax=ax1)
ax1.set_title('Distribution of Cash Metrics')
ax1.tick_params(axis='x', rotation=45)

# Company Performance Metrics
sns.boxplot(data=company_metrics, ax=ax2)
ax2.set_title('Distribution of Company Performance Metrics')
ax2.tick_params(axis='x', rotation=45)

# Top 10 Companies by Free Cash Flow Yield
top_cash_yield = company_cash_all.nlargest(10, 'mean_5yr_cash_yield')
sns.barplot(data=top_cash_yield.reset_index(), x='CompanyName', y='mean_5yr_cash_yield', ax=ax3)
ax3.set_title('Top 10 Companies by Free Cash Flow Yield')
ax3.tick_params(axis='x', rotation=45)

# Top 10 Companies by ROE
top_roe = company_metrics.nlargest(10, 'median_5yr_roe_pct')
sns.barplot(data=top_roe.reset_index(), x='CompanyName', y='median_5yr_roe_pct', ax=ax4)
ax4.set_title('Top 10 Companies by Return on Equity')
ax4.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

# 3. Correlation Matrix of Key Metrics
# Combine relevant metrics
correlation_metrics = pd.merge(
    company_cash_all.reset_index(),
    company_metrics.reset_index(),
    on=['Ticker', 'CompanyName']
)

correlation_cols = [col for col in correlation_metrics.columns
                   if col not in ['Ticker', 'CompanyName']]
correlation_matrix = correlation_metrics[correlation_cols].corr()

plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0)
plt.title('Correlation Matrix of Company Metrics')
plt.tight_layout()
plt.show()



