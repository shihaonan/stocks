import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time

def get_trading_dates(start_date, end_date):
    """获取交易日历"""
    try:
        trading_calendar = ak.tool_trade_date_hist_sina()
        trading_calendar['trade_date'] = pd.to_datetime(trading_calendar['trade_date'])
        mask = (trading_calendar['trade_date'] >= pd.to_datetime(start_date)) & \
               (trading_calendar['trade_date'] <= pd.to_datetime(end_date))
        return trading_calendar[mask]['trade_date'].dt.strftime('%Y-%m-%d').tolist()
    except Exception as e:
        print(f"获取交易日历失败: {e}")
        return []
    
a=get_trading_dates("20240805", "20240818")
print(a)