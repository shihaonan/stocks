import akshare as ak

# stock_financial_abstract_ths_df = ak.stock_financial_abstract_ths(symbol="002693", indicator="按报告期")
# print(stock_financial_abstract_ths_df[["报告期", "营业总收入", "扣非净利润"]].iloc[0])

def convert_to_float(value_str):
    """将带有单位的字符串数据转换为浮点数"""
    if '亿' in value_str:
        return float(value_str.replace('亿', '')) * 100000000
    elif '万' in value_str:
        return float(value_str.replace('万', '')) * 10000
    else:
        return float(value_str)

def get_financial_data(stock_code, date):
    """获取财务数据"""
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

a=get_financial_data("002693", "20240831")
print(a)

