import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import concurrent.futures
import threading
from functools import lru_cache

# 添加线程锁
print_lock = threading.Lock()

def get_trading_stocks():
    """获取主板A股代码列表"""
    try:
        stock_info = ak.stock_info_a_code_name()
        main_board = stock_info[stock_info['code'].str.match('^(600|601|603|000|001|002)')]
        return main_board['code'].tolist()
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return []

def get_stock_data_batch(date):
    """获取指定日期的股票数据"""
    try:
        stocks = get_trading_stocks()
        stock_data = {}
        for stock in stocks:
            df = ak.stock_zh_a_hist(
                symbol=stock,
                start_date=date.replace('-', ''),
                end_date=date.replace('-', ''),
                period="daily"
            )
            if not df.empty:
                stock_data[stock] = df.iloc[0]
            time.sleep(0.1)
        return pd.DataFrame(stock_data).T
    except Exception as e:
        print(f"获取{date}的股票数据失败: {e}")
        return pd.DataFrame()

def get_financial_data_batch(date):
    """获取指定日期的财务数据"""
    try:
        stocks = get_trading_stocks()
        financial_data = {}
        for stock in stocks:
            df = ak.stock_financial_report_em(symbol=stock)
            if not df.empty:
                df['日期'] = pd.to_datetime(df['日期'])
                historical_data = df[df['日期'] <= pd.to_datetime(date)]
                if not historical_data.empty:
                    latest_data = historical_data.iloc[0]
                    financial_data[stock] = {
                        'revenue': float(latest_data['营业收入']),
                        'net_profit': float(latest_data['净利润'])
                    }
            time.sleep(0.1)
        return financial_data
    except Exception as e:
        print(f"获取财务数据失败: {e}")
        return {}

def get_major_holder(stock_code, date, max_retries=3):
    """获取指定日期的大股东持股数据"""
    for attempt in range(max_retries):
        try:
            holders_df = ak.stock_holder_change(symbol=stock_code)
            if not holders_df.empty:
                holders_df['变动日期'] = pd.to_datetime(holders_df['变动日期'])
                historical_data = holders_df[holders_df['变动日期'] <= pd.to_datetime(date)]
                if not historical_data.empty:
                    ratio = historical_data.iloc[0]['持股比例']
                    if isinstance(ratio, str):
                        ratio = float(ratio.replace('%', ''))
                    return ratio
            time.sleep(0.5)
        except Exception as e:
            if attempt == max_retries - 1:
                with print_lock:
                    print(f"获取股票{stock_code}股东信息失败: {e}")
            time.sleep(1)
    return 0

def get_trading_data(stock_code, date, max_retries=3):
    """获取交易数据"""
    for attempt in range(max_retries):
        try:
            end_date = pd.to_datetime(date)
            start_date = end_date - timedelta(days=10)
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                start_date=start_date.strftime('%Y%m%d'),
                end_date=end_date.strftime('%Y%m%d'),
                period="daily"
            )
            if not df.empty:
                return df.tail(6)
            time.sleep(0.5)
        except Exception as e:
            if attempt == max_retries - 1:
                with print_lock:
                    print(f"获取股票{stock_code}交易数据失败: {e}")
            time.sleep(1)
    return None

def check_volume_conditions(trading_data):
    """检查成交量条件"""
    try:
        if len(trading_data) < 5:
            return False
        
        current_volume = trading_data['成交量'].iloc[-1]
        last_volume = trading_data['成交量'].iloc[-2]
        avg_volume = trading_data['成交量'].iloc[-5:].mean()
        
        return current_volume > 2 * last_volume and current_volume > 3 * avg_volume
    except Exception as e:
        print(f"检查成交量条件时发生错误: {e}")
        return False

def check_limit_up(trading_data):
    """检查是否有一字涨停"""
    try:
        if len(trading_data) < 2:
            return True
        recent_data = trading_data.tail(2)
        for _, row in recent_data.iterrows():
            if abs(row['开盘'] - row['收盘']) < 0.01:  # 考虑误差
                return True
        return False
    except Exception as e:
        print(f"检查一字涨停时发生错误: {e}")
        return True

def get_trading_dates(start_date, end_date, max_retries=3):
    """获取交易日历"""
    for attempt in range(max_retries):
        try:
            trading_calendar = ak.tool_trade_date_hist_sina()
            trading_calendar['trade_date'] = pd.to_datetime(trading_calendar['trade_date'])
            mask = (trading_calendar['trade_date'] >= pd.to_datetime(start_date)) & \
                   (trading_calendar['trade_date'] <= pd.to_datetime(end_date))
            return trading_calendar[mask]['trade_date'].dt.strftime('%Y-%m-%d').tolist()
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"获取交易日历失败: {e}")
            time.sleep(1)
    return []

def process_single_stock(stock, date, stock_data_batch, financial_data_batch):
    """处理单个股票的逻辑"""
    try:
        # 从批量数据中获取股票信息
        if stock not in stock_data_batch.index:
            return None
            
        stock_info = stock_data_batch.loc[stock]
        market_cap = float(stock_info['总市值']) if '总市值' in stock_info else float('inf')
        
        # 快速筛选
        if market_cap > 3000000000:  # 市值大于30亿
            return None
            
        # 从批量财务数据中获取信息
        financial = financial_data_batch.get(stock)
        if not financial or financial['revenue'] > 200000000 or financial['net_profit'] > 3000000:
            return None
        
        # 获取大股东持股
        major_holder_ratio = get_major_holder(stock, date)
        if major_holder_ratio < 30:
            return None
        
        # 获取交易数据并检查条件
        trading_data = get_trading_data(stock, date)
        if trading_data is None or len(trading_data) < 5:
            return None
        
        if check_volume_conditions(trading_data) and not check_limit_up(trading_data):
            with print_lock:
                print(f"股票{stock}满足所有条件")
            return stock
            
    except Exception as e:
        with print_lock:
            print(f"处理股票 {stock} 时发生错误: {e}")
    return None

def run_daily_selection(date):
    """每个交易日的选股逻辑"""
    print(f"\n开始处理日期: {date}")
    
    # 批量获取数据
    stock_data_batch = get_stock_data_batch(date)
    financial_data_batch = get_financial_data_batch(date)
    
    stocks = get_trading_stocks()
    selected_stocks = []
    
    # 使用线程池处理股票
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_stock = {
            executor.submit(
                process_single_stock, 
                stock, 
                date, 
                stock_data_batch, 
                financial_data_batch
            ): stock for stock in stocks
        }
        
        for future in concurrent.futures.as_completed(future_to_stock, timeout=300):
            stock = future_to_stock[future]
            try:
                result = future.result(timeout=60)
                if result:
                    selected_stocks.append(result)
            except Exception as e:
                with print_lock:
                    print(f"处理股票 {stock} 时发生错误: {e}")
    
    return {date: selected_stocks}

def save_results(results):
    """保存结果到CSV文件"""
    try:
        rows = []
        for date, stocks in results.items():
            if stocks:
                for stock in stocks:
                    rows.append({'日期': date, '股票代码': stock})
        
        if rows:
            df = pd.DataFrame(rows)
            filename = f"选股结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"\n结果已保存到文件: {filename}")
    except Exception as e:
        print(f"保存结果时发生错误: {e}")

def main(start_date, end_date):
    """主函数"""
    trading_dates = get_trading_dates(start_date, end_date)
    if not trading_dates:
        print("未获取到交易日期")
        return
    
    all_results = {}
    
    for date in trading_dates:
        try:
            result = run_daily_selection(date)
            all_results.update(result)
            
            stocks = result[date]
            if stocks:
                print(f"\n{date} 满足条件的股票：{stocks}")
            else:
                print(f"\n{date} 没有满足条件的股票")
                
        except Exception as e:
            print(f"处理日期 {date} 时发生错误: {e}")
            continue
    
    save_results(all_results)

if __name__ == "__main__":
    start_date = '2024-08-05'
    end_date = '2024-11-28'
    
    print(f"开始回溯选股 - 从 {start_date} 到 {end_date}")
    main(start_date, end_date)