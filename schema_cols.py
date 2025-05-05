def schema_float():
    return ["AverageVolume","Beta","Discounted_Cash_Flow","Discounted_Cash_Flow_Diff", "Asset_Turnover",
            "Capex_Per_Share", "Capex_To_Coverage_Ratio", "Capex_To_Operating_CashFlow_PCT",
            "Capex_To_Revenue_PCT", "Cash_Conversion_Cycle", "Cash_Per_Share", "Current_Ratio",
            "Days_Of_Inventory_OnHand", "Debt_To_Assets_PCT", "Debt_To_Equity_PCT",
            "Debt_To_Market_Cap_PCT", "Dividend_Per_Share", "Dividend_Yield_PCT", "EBIT_Per_Revenue",
            "Effective_Tax_Rate_PCT", "Enterprise_Value_Multiple", "Free_Cash_Flow_Per_Share",
            "Free_Cash_Flow_Yield_PCT", "Intangibles_To_TotalAssets", "Interest_Coverage",
            "Long_Term_Debt_To_Capitalization", "Net_Current_Assets", "Net_Debt_To_EBITDA",
            "Net_Income_Per_Share", "Net_Profit_Margin_PCT", "Operating_CashFlow_To_Sales_Ratio",
            "Operating_Cash_Flow_Per_Share", "Operating_Cycle", "Operating_Profit_Margin",
            "Payables_Turnover", "Pre_Tax_Profit_Margin_PCT", "Price_To_Book,Price_To_Earnings",
            "Price_To_Fair_Value", "Price_To_Sales_Ratio", "Quick_Ratio", "Receivables_Turnover",
            "Return_On_Capital_Employed_PCT", "Return_On_Equity_PCT", "Return_On_Invested_Capital_PCT",
            "Revenue_Per_Share", "Shareholders_Equity_Per_Share", "Short_Term_Coverage_Ratios",
            "Stock_Compensation_To_Revenue", "Tangible_Asset_Value", "Tangible_Book_Value_Per_Share",
            "Total_Debt_To_Capitalization_PCT", "Working_Capital_Local_Currency",  "Debt_Growth_PCT",
            "Dividend_Per_Share_Growth", "EBIT_Growth_PCT", "EPS_Diluted_Growth_PCT", "EPS_Growth_PCT",
            "Five_Year_Dividend_Per_Share_Growth", "Five_Year_Net_Income_Growth_Per_Share",
            "Five_Year_Operating_CashFlow_Growth_Per_Share", "Five_Year_Revenue_Growth_Per_Share",
            "Five_Year_Shareholders_Equity_Growth_Per_Share", "Free_CashFlow_Growth_PCT", "Gross_Profit_Growth_PCT",
            "Inventory_Growth_PCT", "Net_Income_Growth_PCT", "Operating_CashFlow_Growth_PCT", "Operating_Income_Growth_PCT",
            "Revenue_Growth_PCT", "RD_Expense_Growth_PCT", "Receivables_Growth_PCT", "SGA_Expense_Growth_PCT",
            "Three_Year_Shareholder_Equity_Growth_Per_Share", "Three_Year_Revenue_Growth_Per_Share",
            "Three_Year_Operating_CashFlow_Growth_Per_Share", "Three_Year_Net_Income_Growth_Per_Share",
            "Three_Year_Dividend_Per_Share_Growth", "Ten_Year_Shareholder_Equity_Growth_Per_Share",
            "Ten_Year_Revenue_Growth_Per_Share", "Ten_Year_Operating_CashFlow_Growth_Per_Share",
            "Ten_Year_Net_Income_Growth_Per_Share", "Ten_Year_Dividend_Per_Share_Growth", "Gross_Profit_Margin",
            "Price_To_Book", "Price_To_Earnings", "Price_To_Free_Cash_Flow", "Prince_To_Operating_Cash_Flow",
            "R_And_D_ToRevenue", "EBITDA_Growth_PCT", "Debt_To_Equity", "Debt_To_Asset", "Debt_To_EBITDA",
            "Debt_To_EBIT", "Debt_To_Operating_CashFlow", "EBIT_Growth_PCT", "EBITDA_Growth_PCT", "Debt_To_Revenue",
            "Debt_To_Shareholders_Equity", "Equity_To_Asset", "Free_CashFlow_Per_Share", "Operating_CashFlow_Per_Share",
            "Three_Year_Shareholders_Equity_Growth_Per_Share", "Three_Year_Revenue_Growth_Per_Share", "Three_Year_Operating_CashFlow_Growth_Per_Share",
            "Three_Year_Net_Income_Growth_Per_Share", "Three_Year_Dividend_Per_Share_Growth", "Ten_Year_Shareholders_Equity_Growth_Per_Share",
            "Ten_Year_Revenue_Growth_Per_Share", "Ten_Year_Operating_CashFlow_Growth_Per_Share", "Ten_Year_Net_Income_Growth_Per_Share",
            "Ten_Year_Dividend_Per_Share_Growth"]

def schema_date():
    return ["IPO_Date", "Date", "EstimateDate"]

def schema_bool():
    return ["ADR","isETF","isFund"]

def schema_int():
    return ["CalendarYear"]

def schema_datetime():
    return ["AsOfDate"]