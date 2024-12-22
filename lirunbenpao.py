
# 本策略为 www.joinquant.com/post/47346 的改进版本
# 根据国九条，筛选股票
# 导入所需的函数库
from jqdata import *
from jqfactor import *
import numpy as np
import pandas as pd
from datetime import time
from jqdata import finance

#import datetime
# 初始化函数
def initialize(context):
    """
    初始化策略参数和设置。
    """
    # 开启防未来数据功能
    set_option('avoid_future_data', True)
    # 设定基准指数为深证成指
    set_benchmark('399101.XSHE')
    # 使用真实价格进行交易
    set_option('use_real_price', True)
    # 设置滑点为0.001
    set_slippage(FixedSlippage(0.001))
    # 设置交易成本，包括开仓税、平仓税、佣金等
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=2.5/10000, close_commission=2.5/10000, close_today_commission=0, min_commission=5), type='stock')
    # 设置日志级别，过滤低于error级别的日志
    log.set_level('order', 'error')
    log.set_level('system', 'error')
    log.set_level('strategy', 'debug')
    
    # 初始化全局变量
    g.trading_signal = True  # 是否为可交易日
    g.run_stoploss = True  # 是否进行止损
    g.filter_audit = False  # 是否筛选审计意见
    g.filter_bons = False  # 是否筛选红利
    g.adjust_num = True  # 是否调整持仓数量
    
    # 初始化全局列表
    g.hold_list = []  # 当前持仓的全部股票
    g.yesterday_HL_list = []  # 记录持仓中昨日涨停的股票
    g.target_list = []  # 目标股票列表
    g.pass_months = [1, 4]  # 空仓的月份
    g.limitup_stocks = []  # 记录涨停的股票避免再次买入
    
    # 初始化全局数值
    g.min_mv = 3  # 股票最小市值要求（单位：亿元）
    g.max_mv = 1000  # 股票最大市值要求（单位：亿元）
    g.stock_num = 40  # 持股数量
    g.reason_to_sell = {}  # 卖出原因
    g.stoploss_strategy = 3  # 止损策略：1为止损线止损，2为市场趋势止损, 3为联合1、2策略
    g.stoploss_limit = 0.07  # 止损线（百分比）
    g.stoploss_market = 0.05  # 市场趋势止损参数（百分比）
    g.highest = 60  # 股票单价上限设置
    g.etf = '511880.XSHG'  # 空仓月份持有银华日利ETF
    
    # 设置交易运行时间
    run_daily(prepare_stock_list, '9:05')  # 准备股票池
    run_daily(trade_afternoon, time='14:00', reference_security='399101.XSHE')  # 检查持仓中的涨停股是否需要卖出
    run_daily(sell_stocks, time='10:00')  # 止损函数
    run_daily(close_account, '14:50')  # 清仓
    run_weekly(weekly_adjustment, 2, '10:00')  # 周期性调整持仓
    # run_weekly(print_position_info, 5, time='15:10', reference_security='000300.XSHG')  # 打印持仓信息

# 1-1 准备股票池
def prepare_stock_list(context):
    """
    准备股票池，获取当前持仓列表和昨日涨停列表，并判断今天是否为可交易日。
    """
    # 获取已持有股票列表
    g.hold_list = []
    g.limitup_stocks = []
    for position in list(context.portfolio.positions.values()):
        stock = position.security
        g.hold_list.append(stock)
    
    # 获取昨日涨停股票列表
    if g.hold_list:
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close', 'high_limit', 'low_limit'], count=1, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]
        g.yesterday_HL_list = list(df.code)
    else:
        g.yesterday_HL_list = []
    
    # 判断今天是否为可交易日
    g.trading_signal = today_is_between(context)

# 1-2 选股模块
def get_stock_list(context):
    """
    选股模块，根据筛选条件选择目标股票列表。
    """
    final_list = []
    MKT_index = '399101.XSHE'
    # 获取中小综指指数的成分股，然后筛选一下
    # initial_list = filter_stocks(context, get_index_stocks(MKT_index))
    initial_list = filter_stocks(context, get_all_securities(types=['stock'], date=context.current_dt.date()))
    
    # 查询符合条件的股票
    q = query(
        valuation.code,
        valuation.market_cap,  # 总市值
        income.np_parent_company_owners,  # 归属于母公司所有者的净利润
        income.net_profit,  # 净利润
        income.operating_revenue  # 营业收入
    ).filter(
        valuation.code.in_(initial_list),
        valuation.market_cap.between(g.min_mv, g.max_mv),
        income.np_parent_company_owners > 0,
        income.net_profit > 0,
        income.operating_revenue > 1e8
    ).order_by(valuation.market_cap.asc()).limit(g.stock_num * 3)
    
    df = get_fundamentals(q)
    
    # 过滤审计意见有问题的股票
    if g.filter_audit:
        before_audit_filter = len(df)
        df['audit'] = df['code'].apply(lambda x: filter_audit(context, x))
        df_audit = df[df['audit'] == True]
        log.info('去除掉了存在审计问题的股票{}只'.format(len(df) - before_audit_filter))
    
    # 过滤红利股
    if g.filter_bons:
        final_list = bons_filter(context, list(df.code))
        print(f'过滤红利前的股票列表{list(df.code)}')
        print(f'过滤红利后的股票列表{final_list}')
    else:
        final_list = list(df.code)
    
    print(len(final_list))
    
    last_prices = history(1, unit='1d', field='close', security_list=final_list)
    
    if len(final_list) == 0:
        log.info('无适合股票，买入ETF')
        return [g.etf]
    else:
        return [stock for stock in final_list if stock in g.hold_list or last_prices[stock][-1] <= g.highest]

# 1-3 整体调整持仓
def weekly_adjustment(context):
    """
    每周调整持仓，根据市场情况和策略参数调整持股数量和目标股票列表。
    """
    if g.trading_signal and g.adjust_num:
        new_num = adjust_stock_num(context)
        if new_num == 0:
            buy_security(context, [g.etf], 1)
            log.info('MA指示指数大跌，持有银华日利ETF')
        else:
            if g.stock_num != new_num:
                g.stock_num = new_num
                log.info(f'持仓数量修改为{new_num}')
            # 获取目标股票列表，然后取前 g.stock_num 个元素
            g.target_list = get_stock_list(context)[:g.stock_num]
            log.info(str(g.target_list))
            
            # 卖出当前持仓中，不在目标股票列表 并且也不在昨日涨停股票列表的股票
            sell_list = [stock for stock in g.hold_list if stock not in g.target_list and stock not in g.yesterday_HL_list]
            # 从 g.hold_list 中筛选出那些在目标股票列表，或者在昨日涨停股票列表中的股票，作为应该继续持仓股票
            hold_list = [stock for stock in g.hold_list if stock in g.target_list or stock in g.yesterday_HL_list]
            log.info("卖出[%s]" % (str(sell_list)))
            log.info("已持有[%s]" % (str(hold_list)))
            
            # 构建要卖出的持仓信息列表
            sell_positions = [context.portfolio.positions[stock] for stock in sell_list]
            for position in sell_positions:
                # 逐个清仓
                close_position(position)
            
            # 选择在目标股票列表中但不在当前持仓中的，作为待买入列表
            buy_list = [stock for stock in g.target_list if stock not in g.hold_list]
            buy_security(context, buy_list, len(buy_list))
            
            for position in list(context.portfolio.positions.values()):
                stock = position.security
    else:
        buy_security(context, [g.etf], 1)
        log.info('该月份为空仓月份，持有银华日利ETF')


# 1-4 调整昨日涨停股票
def check_limit_up(context):
    """
    检查昨日涨停股票在当前交易日的表现，如未涨停则提前卖出。
    """
    now_time = context.current_dt
    if g.yesterday_HL_list:
        for stock in g.yesterday_HL_list:
            # 获取昨日涨停股的最新价和今日涨停价
            current_data = get_price(stock, end_date=now_time, frequency='1m', fields=['close', 'high_limit'], skip_paused=False, fq='pre', count=1, panel=False, fill_paused=True)
           # 如果最新价小于涨停价
            if current_data.iloc[0, 0] < current_data.iloc[0, 1]:
                log.info("[%s]涨停打开，卖出" % (stock))
                position = context.portfolio.positions[stock]
                close_position(position)
                g.reason_to_sell[stock] = 'limitup'
                g.limitup_stocks.append(stock)
            else:
                log.info("[%s]涨停，继续持有" % (stock))

# 1-5 如果昨天有股票卖出或者买入失败造成空仓，剩余的金额当日买入
def check_remain_amount(context):
    """
    检查是否有剩余资金，并根据需要买入股票或货币ETF。
    """
    stoploss_list = []
    uplimit_list = []
    # 迭代卖出原因，取出因为未涨停和止损卖出的股票
    for key, value in g.reason_to_sell.items():
        if value == 'stoploss':
            stoploss_list.append(key)
        elif value == 'limitup':
            uplimit_list.append(key)
    
    # 因未涨停而卖出，而空出来的仓位，还要买新票
    # 因止损而卖出，而空出来的仓位，买etf
    empty_num = len(stoploss_list) + len(uplimit_list)
    addstock_num = len(uplimit_list)
    etf_num = len(stoploss_list)
   
    g.hold_list = context.portfolio.positions
    if len(g.hold_list) < g.stock_num:
        # 计算需要买入的股票数量
        num_stocks_to_buy = min(addstock_num, g.stock_num - len(g.hold_list))
        # 从之前筛选出来的目标股票列表中筛选出那些不在 g.limitup_stocks 列表中的股票，并且取num_stocks_to_buy个
        target_list = [stock for stock in g.target_list if stock not in g.limitup_stocks][:num_stocks_to_buy]
        log.info('有余额可用{}元。买入{}'.format(round(context.portfolio.cash, 2), target_list))
        buy_security(context, target_list, len(target_list))
        
        if etf_num != 0:
            log.info('有余额可用{}元。买入货币基金{}'.format(round(context.portfolio.cash, 2), g.etf))
            buy_security(context, [g.etf], etf_num)
    
    g.reason_to_sell = {}

# 1-6 下午检查交易
def trade_afternoon(context):
    """
    下午检查交易，包括检查涨停股和剩余资金。
    """
    if g.trading_signal:
        check_limit_up(context)
        check_remain_amount(context)
        
# 1-7 止盈止损
def sell_stocks(context):
    """
    止盈止损函数，根据设定的策略进行个股止盈止损。
    """
    if g.run_stoploss:
        current_positions = context.portfolio.positions
        
        # 根据止损策略1或3进行个股止盈止损
        if g.stoploss_strategy in [1, 3]:
            for stock in current_positions.keys():
                price = current_positions[stock].price
                avg_cost = current_positions[stock].avg_cost
                
                # 个股盈利止盈
                if price >= avg_cost * 2:
                    order_target_value(stock, 0)
                    log.debug("收益100%止盈,卖出{}".format(stock))
                
                # 个股止损
                elif price < avg_cost * (1 - g.stoploss_limit):
                    order_target_value(stock, 0)
                    log.debug("收益止损,卖出{}".format(stock))
                    g.reason_to_sell[stock] = 'stoploss'
        
        # 根据止损策略2或3进行市场趋势止损
        if g.stoploss_strategy in [2, 3]:
            stock_df = get_price(security=get_index_stocks('399101.XSHE'), end_date=context.previous_date, frequency='daily', fields=['close', 'open'], count=1, panel=False)
            # 计算成分股平均涨跌，即指数涨跌幅
            down_ratio = (1 - stock_df['close'] / stock_df['open']).mean()
            
            # 市场大跌止损
            if down_ratio >= g.stoploss_market:
                g.reason_to_sell[stock] = 'stoploss'
                log.debug("大盘惨跌,平均降幅{:.2%}".format(down_ratio))
                for stock in current_positions.keys():
                    order_target_value(stock, 0)


# 1-8 动态调仓代码
def adjust_stock_num(context):
    """
    动态调整持仓数量，根据市场指数的移动平均线变化。
    """
    ma_para = 10  # 设置MA参数
    today = context.previous_date
    start_date = today - datetime.timedelta(days=ma_para*2)
    index_df = get_price('399101.XSHE', start_date=start_date, end_date=today, frequency='daily')
    index_df['ma'] = index_df['close'].rolling(window=ma_para).mean()
    last_row = index_df.iloc[-1]
    diff = last_row['close'] - last_row['ma'] # 计算前一个交易日的收盘价与移动平均线之间的差值
    
    # 根据差值结果返回新的持股数量
    result = 3 if diff >= 500 else \
             3 if 200 <= diff < 500 else \
             4 if -200 <= diff < 200 else \
             5 if -500 <= diff < -200 else \
             6
    return result
    

# 2 过滤各种股票
def filter_stocks(context, stock_list):
    """
    过滤不符合条件的股票，包括停牌、ST股、退市股、市场类型不符、涨停、跌停、次新股等。
    """
    current_data = get_current_data()
    # 获取股票列表中前1分钟的收盘价
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    filtered_stocks = []
    
    for stock in stock_list:
        if current_data[stock].paused:  # 停牌
            continue
        if current_data[stock].is_st:  # ST股
            continue
        if '退' in current_data[stock].name:  # 退市股
            continue
        if stock.startswith(('30', '68', '8', '4')):  # 市场类型不符
            continue
        if not (stock in context.portfolio.positions or last_prices[stock][-1] < current_data[stock].high_limit):  # 涨停
            continue
        if not (stock in context.portfolio.positions or last_prices[stock][-1] > current_data[stock].low_limit):  # 跌停
            continue
        # 过滤次新股
        start_date = get_security_info(stock).start_date
        if context.previous_date - start_date < timedelta(days=375):
            continue
        filtered_stocks.append(stock)
    
    return filtered_stocks

# 2.1 筛选审计意见
def filter_audit(context, code):
    """
    筛选审计意见，近三年内如果有不合格的审计意见则返回False，否则返回True。
    """
    lstd = context.previous_date
    last_year = (lstd.replace(year=lstd.year - 3, month=1, day=1)).strftime('%Y-%m-%d')
    q = query(finance.STK_AUDIT_OPINION).filter(finance.STK_AUDIT_OPINION.code == code, finance.STK_AUDIT_OPINION.pub_date >= last_year)
    df = finance.run_query(q)
    df['report_type'] = df['report_type'].astype(str)
    contains_nums = df['report_type'].str.contains(r'2|3|4|5')
    return not contains_nums.any()

# 2.2 红利筛选
def bons_filter(context, stock_list):
    """
    筛选过去三年累计分红大于平均净利润30%且累计分红超过5000万元的股票。
    """
    time1 = context.previous_date
    time2 = time1 - datetime.timedelta(days=365)
    time3 = time1 - datetime.timedelta(days=365*3)
    
    # 获取去年未分配利润大于0的股票
    q = query(
        finance.STK_BALANCE_SHEET.code, 
        finance.STK_BALANCE_SHEET.pub_date, 
        finance.STK_BALANCE_SHEET.retained_profit
    ).filter(
        finance.STK_BALANCE_SHEET.pub_date >= time2,
        finance.STK_BALANCE_SHEET.pub_date <= time1,
        finance.STK_BALANCE_SHEET.retained_profit > 0,
        finance.STK_BALANCE_SHEET.code.in_(stock_list))
    df = finance.run_query(q)
    df = df.set_index('code')
    df = df.groupby('code').mean()
    check_list = list(df.index)

    # 获取分红数据
    q = query(
        finance.STK_XR_XD.code, 
        finance.STK_XR_XD.a_registration_date, 
        finance.STK_XR_XD.bonus_amount_rmb
    ).filter(
        finance.STK_XR_XD.a_registration_date >= time3,
        finance.STK_XR_XD.a_registration_date <= time1,
        finance.STK_XR_XD.code.in_(check_list))
    bons_df = finance.run_query(q)
    bons_df = bons_df.fillna(0)
    bons_df = bons_df.set_index('code')
    bons_df = bons_df.groupby('code').sum()
    bons_list = list(bons_df.index)
    # 获取过去三年平均净利润
    if time1.month >= 5:  # 5月后取去年
        start_year = str(time1.year - 1)
    else:  # 5月前取前年
        start_year = str(time1.year - 2)
    # 获取3年净利润数据
    np = get_history_fundamentals(bons_list, fields=[income.net_profit], watch_date=None, stat_date=start_year, count=3, interval='1y', stat_by_year=True)
    np = np.set_index('code')
    np = np.groupby('code').mean()
    # 获取市值相关数据
    q = query(valuation.code, valuation.market_cap).filter(valuation.code.in_(bons_list))
    cap = get_fundamentals(q, date=time1)
    cap = cap.set_index('code')
    # 筛选过去三年累计分红大于平均净利润的30%且累计分红>5000万
    DR = pd.concat([bons_df, np, cap], axis=1, sort=False)
    DR = DR[((DR['bonus_amount_rmb'] * 10000) > (DR['net_profit'] * 0.3)) | (DR['bonus_amount_rmb'] > 5000)]
    final_list = list(DR.index)
    return final_list
    
# 3-1 交易模块-自定义下单
def order_target_value_(security, value):
    """
    自定义下单函数。
    """
    if value == 0:
        pass
        # log.debug("Selling out %s" % (security))
    else:
        log.debug("Order %s to value %f" % (security, value))
    return order_target_value(security, value)

# 3-2 交易模块-开仓
def open_position(security, value):
    """
    开仓函数。
    """
    order = order_target_value_(security, value)
    if order is not None and order.filled > 0:
        return True
    return False

# 3-3 交易模块-平仓
def close_position(position):
    """
    平仓函数。
    """
    security = position.security
    order = order_target_value_(security, 0)  # 可能会因停牌失败
    if order is not None:
        if order.status == OrderStatus.held and order.filled == order.amount:
            return True
    return False

# 3-4 买入模块
def buy_security(context, target_list, num):
    """
    买入股票。
    """
    position_count = len(context.portfolio.positions)
    target_num = num
    if target_num != 0:
        value = context.portfolio.cash / target_num
        for stock in target_list:
            open_position(stock, value)
            log.info("买入[%s]（%s元）" % (stock, value))
            if len(context.portfolio.positions) == g.stock_num:
                break

# 3-4 买入模块2
def buy_security2(context, target_list):
    """
    买入股票（备用方法）。
    """
    position_count = len(context.portfolio.positions)
    target_num = len(target_list)
    if target_num > position_count:
        value = context.portfolio.cash / (target_num - position_count)
        for stock in target_list:
            if context.portfolio.positions[stock].total_amount == 0:
                if open_position(stock, value):
                    log.info("买入[%s]（%s元）" % (stock, value))
                    if len(context.portfolio.positions) == target_num:
                        break


#4-1 判断今天是否跳过月份
def today_is_between(context):
    """
    判断今天是否跳过月份。
    """
    # 根据g.pass_month跳过指定月份
    month = context.current_dt.month
    # 判断当前月份是否在指定月份范围内
    if month in g.pass_months:
        # 判断当前日期是否在指定日期范围内
        return False
    else:
        return True

#4-2 清仓后次日资金可转
def close_account(context):
    """
    清仓后次日资金可转。
    """
    if g.trading_signal == False:
        if len(g.hold_list) != 0 and g.hold_list != [g.etf]:
            for stock in g.hold_list:
                position = context.portfolio.positions[stock]
                close_position(position)
                log.info("卖出[%s]" % (stock))


def print_position_info(context):
    """
    打印持仓信息。
    """
    for position in list(context.portfolio.positions.values()):
        securities = position.security
        cost = position.avg_cost
        price = position.price
        ret = 100 * (price / cost - 1)
        value = position.value
        amount = position.total_amount    
        print('代码:{}'.format(securities))
        print('成本价:{}'.format(format(cost,'.2f')))
        print('现价:{}'.format(price))
        print('收益率:{}%'.format(format(ret,'.2f')))
        print('持仓(股):{}'.format(amount))
        print('市值:{}'.format(format(value,'.2f')))
    print('———————————————————————————————————————分割线————————————————————————————————————————')
