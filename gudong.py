import requests
import pandas as pd
from datetime import datetime, timedelta
import akshare as ak


def get_major_holder(stock_code, target_date):
    """获取第一大股东持股比例，如果当天没有数据就往前查询"""
    # 处理股票代码前缀
    if stock_code.startswith(('sh', 'sz')):
        formatted_code = stock_code
    else:
        # 上交所股票以6开头，深交所股票以0或3开头
        if stock_code.startswith('6'):
            formatted_code = 'sh' + stock_code
        elif stock_code.startswith(('0', '3')):
            formatted_code = 'sz' + stock_code
        else:
            print(f"无效的股票代码: {stock_code}")
            return None
    # 转换日期为datetime对象，方便进行日期计算
    current_date = pd.to_datetime(target_date)
    
    # 最多往前查询100天
    for _ in range(100):
        try:
            date_str = current_date.strftime('%Y%m%d')
            holders = ak.stock_gdfx_top_10_em(symbol=formatted_code, date=date_str)
            ratio = float(holders.iloc[0]['占总股本持股比例'])
            return ratio
        except:
            # 有任何问题直接忽略，查询前一天
            current_date = current_date - pd.Timedelta(days=1)
            continue
    
    print(f"未能找到股票{formatted_code}的股东数据")
    return None

# 可以测试不同形式的股票代码
test_codes = ['002693', 'sz002693', '600001', 'sh600001']
for code in test_codes:
    result = get_major_holder(code, '20240831')
    if result is not None:
        print(f"股票{code}的第一大股东持股比例: {result}%")