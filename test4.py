import requests
import pandas as pd
from datetime import datetime, timedelta
import akshare as ak

import requests
import pandas as pd
from datetime import datetime, timedelta


def get_major_holder(stock_code, target_date):
    """获取第一大股东持股比例，如果当天没有数据就往前查询"""
    # 转换日期为datetime对象，方便进行日期计算
    current_date = pd.to_datetime(target_date)
    
    # 最多往前查询100天
    for _ in range(100):
        try:
            date_str = current_date.strftime('%Y%m%d')
            holders = ak.stock_gdfx_top_10_em(symbol=stock_code, date=date_str)
            # print(target_date,holders)
            
            if not holders.empty:
                # 成功获取数据
                first_holder = holders.iloc[0]
                print(first_holder)
                ratio = float(first_holder['持股比例'].replace('%', ''))
                print(f"找到数据日期: {date_str}")
                return ratio
                
        except Exception as e:
            pass  # 忽略错误，继续尝试前一天
            
        # 日期往前推一天
        current_date = current_date - pd.Timedelta(days=1)
    
    print(f"未能找到股票{stock_code}的股东数据")
    return None

# 测试
result = get_major_holder('sz002693', '20240831')
if result is not None:
    print(f"第一大股东持股比例: {result}%")

# stock_gdfx_top_10_em_df = ak.stock_gdfx_top_10_em(symbol="sz002693")
# print(stock_gdfx_top_10_em_df)

# stock_shareholder_change_ths_df = ak.stock_management_change_ths(symbol="002693")
# print(stock_shareholder_change_ths_df)