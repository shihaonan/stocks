import akshare as ak
import datetime

# 获取当前日期
today = datetime.datetime.now().strftime('%Y%m%d')


# 获取ST股票数据，这里假设只获取主板市场的ST股，可根据实际情况调整
df_stocks = ak.stock_info_a_code_name()

# # 打印ST股票的代码和名称
# for index, row in df_stocks.iterrows():
#     if 'ST' in row['name']:
#         print(f"股票代码: {row['code']}, 股票名称: {row['name']}")


# 存储公告的列表
announcements = []
for index, row in df_stocks.iterrows():
    stock_code = row['code']
    try:
        # 获取该股票的公告数据
        # bug修复: 将接口名更正为正确的名称
        df_announcement = ak.stock_zh_a_disclosure_report_cninfo(symbol=stock_code, market="沪深京", category="公司治理", start_date="20240919", end_date="20241119")


        for _, ann_row in df_announcement.iterrows():
            announcements.append({
                '股票代码': stock_code,
                '公告标题': ann_row['announcement_title'],
                '公告时间': ann_row['announcement_time'],
                '公告链接': ann_row['announcement_url']
            })
    except Exception as e:
        print(f"获取股票 {stock_code} 公告失败: {e}")

# 打印公告信息
for ann in announcements:
    print(ann)
