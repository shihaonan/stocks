import akshare as ak
import pandas as pd


stock_zh_a_disclosure_report_cninfo_df = ak.stock_zh_a_disclosure_report_cninfo(symbol="603007", market="沪深京",start_date="20240101", end_date="20241122")

print(type(stock_zh_a_disclosure_report_cninfo_df))
# print(stock_zh_a_disclosure_report_cninfo_df)
print(stock_zh_a_disclosure_report_cninfo_df.iloc[0:5])

# 判断'公告标题'列是否包含'重整'字眼，并生成布尔掩码
mask = stock_zh_a_disclosure_report_cninfo_df['公告标题'].str.contains('重整')

# 根据布尔掩码提取满足条件的行
result_df = stock_zh_a_disclosure_report_cninfo_df[mask]

print(result_df)