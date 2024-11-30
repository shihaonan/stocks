import time
import akshare as ak
import datetime
import pandas as pd

# def get_major_holder(stock_code, date, max_retries=3):
#     """获取指定日期的大股东持股数据"""
#     for attempt in range(max_retries):
#         try:
#             # 使用 stock_individual_info_em 接口
#             stock_info = ak.stock_individual_info_em(symbol=stock_code)
#             if not stock_info.empty:
#                 # 第一大股东持股比例在 '控股股东持股比例' 列
#                 ratio_str = stock_info[stock_info['item'] == '控股股东持股比例']['value'].iloc[0]
#                 ratio = float(ratio_str.replace('%', ''))
#                 return ratio
#             time.sleep(0.5)
#         except Exception as e:
#             if attempt == max_retries - 1:
#                 print(f"获取股票{stock_code}股东信息失败: {e}")
#             time.sleep(1)
#     return 0

def get_major_holder(stock_code, date):
    """获取第一大股东持股比例"""
    print(date)
    try:
        # 使用 stock_individual_info_em 接口获取主要股东数据
        holders = ak.stock_gdfx_top_10_em(symbol=stock_code,date=date)
        if not holders.empty:
            # 获取第一大股东的持股比例
            print(holders)
            first_holder = holders.iloc[0]
            # print(first_holder)
    except Exception as e:
        print(f"获取股票{stock_code}股东信息失败: {e}")

get_major_holder('sz002693', '20240827')

# stock_gdfx_top_10_em_df = ak.stock_gdfx_top_10_em(symbol="sz002693", date="20240827")
# print(stock_gdfx_top_10_em_df)