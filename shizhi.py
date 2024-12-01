import akshare as ak

# stock_individual_info_em_df = ak.stock_individual_info_em(symbol="000001")
# print(stock_individual_info_em_df)
# print(stock_individual_info_em_df.iloc[2, 1])


# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20240528", end_date='20240528', adjust="")
# print(stock_zh_a_hist_df["收盘"])

def market_value(stock_code, target_date):
    try:
        # 获取股票的总股本信息
        stock_info_df = ak.stock_individual_info_em(symbol=stock_code)
        total_shares = stock_info_df.iloc[2, 1]

        # 获取指定日期的收盘价
        hangqing = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=target_date, end_date=target_date, adjust="")
        print(target_date)
        closing_price = hangqing["收盘"].iloc[0]

        # 计算市值
        market_value = total_shares * closing_price

        return market_value

    except Exception as e:
        print(f"计算股票 {stock_code} 在 {target_date} 的市值时出现错误: {e}")
        return None
    
a=market_value("000001", "20241129")
print(a)
