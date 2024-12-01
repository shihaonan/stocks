import akshare as ak

stock_zh_a_gdhs_df = ak.stock_zh_a_gdhs(symbol="最新").loc[:, ['代码', '总股本']]
# print(stock_zh_a_gdhs_df)

stock_hsgt_stock_statistics_em_df = ak.stock_hsgt_stock_statistics_em(symbol="北向持股", start_date="20241101", end_date="20241101")
print(stock_hsgt_stock_statistics_em_df)