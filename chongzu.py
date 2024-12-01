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

#获取市值
def market_value(stock_code, target_date):
    try:
        # 获取股票的总股本信息
        stock_info_df = ak.stock_individual_info_em(symbol=stock_code)
        total_shares = stock_info_df.iloc[2, 1]

        # 获取指定日期的收盘价
        hangqing = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=target_date, end_date=target_date, adjust="")
        closing_price = hangqing["收盘"].iloc[0]

        # 计算市值
        market_value = total_shares * closing_price
        return market_value

    except Exception as e:
        print(f"计算股票 {stock_code} 在 {target_date} 的市值时出现错误: {e}")
        return None

def convert_to_float(value_str):
    """将带有单位的字符串数据转换为浮点数"""
    if '亿' in value_str:
        return float(value_str.replace('亿', '')) * 100000000
    elif '万' in value_str:
        return float(value_str.replace('万', '')) * 10000
    else:
        return float(value_str)

def get_financial_data(stock_code, date):
    """获取营收和利润数据"""
    try:
        financial = ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按报告期")

        if not financial.empty:
            # 获取最近的财务数据
            latest = financial.iloc[0]
            revenue=convert_to_float(latest['营业总收入'])
            net_profit= convert_to_float(latest['扣非净利润'])
            return {
                'revenue': revenue,
                'net_profit': net_profit
            }

    except Exception as e:
        print(f"获取股票{stock_code}财务数据失败: {e}")
    return None

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
        return trading_calendar[mask]['trade_date'].dt.strftime('%Y%m%d').tolist()
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
            time.sleep(0.5)  # 增加延时以避免被限制
            
            market_cap = market_value(stock, date)
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
                print(f"股票{stock}营收{revenue},净利润{net_profit},过大")
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
    start_date = '20240805'
    end_date = '20240805'
    
    print(f"开始回溯选股 - 从 {start_date} 到 {end_date}")
    main(start_date, end_date)