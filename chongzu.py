import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time

def get_trading_stocks():
    """获取主板A股代码列表"""
    stock_info = ak.stock_info_a_code_name()
    # 排除创业板、科创板等
    main_board = stock_info[stock_info['code'].str.match('^(600|601|603|000|001|002)')]
    return main_board['code'].tolist()

def get_stock_basic_info(stock_code, date):
    """获取股票基础信息（市值等）"""
    try:
        # 使用 stock_zh_a_spot_em 接口获取实时行情数据
        stock_info = ak.stock_zh_a_spot_em()
        stock_info = stock_info[stock_info['代码'] == stock_code]
        
        if not stock_info.empty:
            # 总市值通常以亿为单位
            market_cap = float(stock_info['总市值'].values[0])
            return {'total_mv': market_cap}
        return None
    except Exception as e:
        print(f"获取股票{stock_code}基础信息失败: {e}")
        return None

def get_financial_data(stock_code, date):
    """获取财务数据"""
    try:
        # 使用 stock_financial_abstract_em 接口获取财务摘要数据
        financial = ak.stock_financial_abstract_em(symbol=stock_code)
        if not financial.empty:
            # 获取最近的财务数据
            latest = financial.iloc[0]
            return {
                'revenue': float(latest['营业收入']),
                'net_profit': float(latest['净利润'])
            }
    except Exception as e:
        print(f"获取股票{stock_code}财务数据失败: {e}")
    return None

def get_major_holder(stock_code, date):
    """获取第一大股东持股比例"""
    try:
        # 使用 stock_main_stock_holder_em 接口获取主要股东数据
        holders = ak.stock_main_stock_holder_em(symbol=stock_code)
        if not holders.empty:
            # 获取第一大股东的持股比例
            first_holder = holders.iloc[0]
            return float(first_holder['持股比例'].replace('%', ''))
    except Exception as e:
        print(f"获取股票{stock_code}股东信息失败: {e}")
    return 0

def get_trading_data(stock_code, date):
    """获取交易数据"""
    try:
        # 使用 stock_zh_a_hist 接口获取历史行情数据
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
    except Exception as e:
        print(f"获取股票{stock_code}交易数据失败: {e}")
    return None

def check_volume_conditions(trading_data):
    """检查成交量条件"""
    if len(trading_data) < 5:
        return False
    
    current_volume = trading_data['成交量'].iloc[-1]
    last_volume = trading_data['成交量'].iloc[-2]
    avg_volume = trading_data['成交量'].iloc[-5:].mean()
    
    return current_volume > 2 * last_volume and current_volume > 3 * avg_volume

def check_limit_up(trading_data):
    """检查是否有一字涨停"""
    if len(trading_data) < 2:
        return True
    recent_data = trading_data.tail(2)
    for _, row in recent_data.iterrows():
        if row['开盘'] == row['收盘']:  # 一字板判断
            return True
    return False


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

def run_daily_selection(date):
    """每个交易日的选股逻辑"""
    print(f"\n开始处理日期: {date}")
    
    stocks = get_trading_stocks()
    selected_stocks = []
    
    # for stock in stocks[:10]:  # 先测试前10只股票
    for stock in stocks:  
        try:
            # 添加进度显示，打印当前股票是正在处理的第几只股票，并显示总数
            print(f"{date}  -   正在处理: {stock}，{stocks.index(stock) + 1}/{len(stocks)}")     
            time.sleep(0.1)  # 增加延时以避免被限制
            
            # 获取基本面数据
            basic_info = get_stock_basic_info(stock, date)
            if basic_info is None:
                print(f"无法获取{stock}的基本面数据")
                continue
                
            market_cap = basic_info.get('total_mv',10000000000)
            if market_cap > 3000000000:  # 市值大于30亿
                print(f"股票{stock}市值{market_cap},过大")
                continue
            
            # 获取大股东持股
            major_holder_ratio = get_major_holder(stock, date)
            if major_holder_ratio < 30:
                print(f"股票{stock}大股东持股比例{major_holder_ratio},过小")
                continue
            
            # 获取财务数据    
            financial = get_financial_data(stock, date)
            if financial is None:
                continue
                
            revenue = financial.get('revenue', 300000000)
            net_profit = financial.get('net_profit', float('300000000'))
            
            if revenue > 200000000 or net_profit > 3000000:
                print(f"股票{stock}营收{revenue},净利润{net_profit},过小")
                continue
            
            # 获取交易数据
            trading_data = get_trading_data(stock, date)
            if trading_data is None or len(trading_data) < 5:
                continue
            
            if check_volume_conditions(trading_data) and not check_limit_up(trading_data):
                selected_stocks.append(stock)
                print(f"股票{stock}满足所有条件")
            
        except Exception as e:
            print(f"处理股票 {stock} 时发生错误: {e}")
            continue
    
    return {date: selected_stocks}

def main(start_date, end_date):
    """主函数"""
    # 获取交易日列表
    trading_dates = get_trading_dates(start_date, end_date)
    if not trading_dates:
        print("未获取到交易日期")
        return
    
    # 存储所有结果
    all_results = {}
    
    # 对每个交易日进行处理
    for date in trading_dates:
        try:
            result = run_daily_selection(date)
            all_results.update(result)
            
            # 打印当天结果
            stocks = result[date]
            if stocks:
                print(f"\n{date} 满足条件的股票：{stocks}")
            else:
                print(f"\n{date} 没有满足条件的股票")
                
        except Exception as e:
            print(f"处理日期 {date} 时发生错误: {e}")
            continue
    
    # 保存结果到CSV文件
    save_results(all_results)

def save_results(results):
    """保存结果到CSV文件"""
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

if __name__ == "__main__":
    # 设置起止日期
    start_date = '2024-08-05'
    end_date = '2024-08-05'
    
    print(f"开始回溯选股 - 从 {start_date} 到 {end_date}")
    main(start_date, end_date)