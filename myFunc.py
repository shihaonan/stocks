# 这是我的聚宽量化的常用函数库

# 获取今日全部股票
def get_all_stocks(context):
    # type: (Context) -> list
    return list(get_all_securities(['stock'], date=context.current_dt).index)



# 筛选出昨日涨停的股票
def filter_limit_up(context, stock_list):
    # type: (Context, list) -> dict
    end_date = context.previous_date
    df_hl = get_price(stock_list, end_date=end_date, frequency='daily',
                       fields=['close', 'high_limit'], count=1, panel=False
                       ).query('close == high_limit').set_index('code')  # 今日涨停
    hl = df_hl.index.tolist()

# 筛选出今日涨停的股票
def filter_limit_up_today(context, stock_list):
    hl_today = []
    curr_data = get_current_data()
    for stock in stock_list:
        current_price = curr_data[stock].last_price
        day_high_limit = curr_data[stock].high_limit
        if current_price == day_high_limit:
            hl_today.append(stock)
    return hl_today



# 过滤各种股票
def filter_stocks(context, stock_list):
    # type: (Context, list) -> list
    # 过滤ST、停牌，创业板、科创板
    current_data = get_current_data()
    filter_stocks_list = [code for code in stock_list if not (
            code.startswith(('3', '68', '4', '8'))
            or current_data[code].is_st
            or current_data[code].paused
            or '退' in current_data[code].name
    )]
    return filter_stocks_list

# 通过财务数据筛选股票
def filter_stocks_by_fundamentals(context, stock_list):
    # type: (Context, list) -> list
    # 过滤掉：流通市值>=150
    stock_list = get_fundamentals(
        query(
            valuation.code
        ).filter(
            valuation.code.in_(initial_list),
            valuation.market_cap.between(g.min_mv,g.max_mv),
            income.np_parent_company_owners > 0,
            income.net_profit > 0,
            income.operating_revenue > 1e8,
        ).order_by(valuation.market_cap.asc())
    )['code'].tolist()
    return stock_list

# 获取股票列表的概念。每个股票有多个概念，每个概念出现一次则计数+1，统计概念出现的次数，最终得到一组数据，列分别是概念名称和出现次数
def get_concepts(context, stock_list):
    # 预定义要过滤的关键词（列表）
    filter_keywords = ['融资融券', '深股通', '国企改革', '转融券标的', '创投', 
                      '独角兽', '上海国资改革', '沪股通', '预增']
    
    # 获取所有股票的概念数据
    concepts_data = get_concept(stock_list, context.current_dt)
    
    # 创建字典来存储概念计数
    concept_dict = {}
    
    # 遍历每个股票的概念
    for stock in concepts_data:
        for concept in concepts_data[stock]['jq_concept']:
            concept_name = concept['concept_name']
            
            # 检查概念名称是否包含任何过滤关键词
            should_filter = False
            for keyword in filter_keywords:
                if keyword in concept_name:  # 模糊匹配，类似SQL的LIKE
                    should_filter = True
                    break
            
            if should_filter:
                continue
                
            concept_code = concept['concept_code']
            
            # 使用字典更新计数
            if concept_code in concept_dict:
                concept_dict[concept_code]['count'] += 1
            else:
                concept_dict[concept_code] = {
                    'concept_code': concept_code,
                    'concept_name': concept_name,
                    'count': 1
                }
    
    # 将字典转换为DataFrame并排序
    if not concept_dict:
        return pd.DataFrame(columns=['concept_code', 'concept_name', 'count'])
        
    df = pd.DataFrame(list(concept_dict.values()))
    df = df.sort_values('count', ascending=False)
    print(df)
    
    return df