DROP TABLE IF EXISTS ads.wholesale_customer_sales_repay_analysis_m;
CREATE TABLE ads.wholesale_customer_sales_repay_analysis_m (
    -- 颗粒度
    stat_yearmonth varchar COMMENT '统计年月',
    entryid bigint COMMENT '独立单元ID',
    customid bigint COMMENT '客户ID',

    -- 维度
    entry_name varchar COMMENT '独立单元名称',
    customer_name varchar COMMENT '客户名称',
    financeclass_name varchar COMMENT '财务类别名称',
    customertype_name varchar COMMENT '客户类型名称',
    customertype_group varchar COMMENT '客户类型分组',
    
    -- 近期销售回款指标（近3个月）
    recent_3m_sales decimal(18,2) COMMENT '近期销售额',
    recent_3m_repay decimal(18,2) COMMENT '近期还款额',
    recent_3m_gross_profit decimal(18,2) COMMENT '近期毛利和',
    
    -- 上月销售回款指标
    last_month_sales decimal(18,2) COMMENT '上月销售额',
    last_month_repay decimal(18,2) COMMENT '上月还款额',
    last_month_gross_profit decimal(18,2) COMMENT '上月毛利和',
    
    -- 未回款指标
    total_unpaid decimal(18,2) COMMENT '历史未还款总和',
    unpaid_order_count int COMMENT '未还款订单数量',
    
    -- 回款时效指标
    recent_3m_avg_repay_days decimal(10,2) COMMENT '近期还款用时',
    recent_3m_jicai_avg_days decimal(10,2) COMMENT '近期集采还款平均用时',
    recent_3m_nonjicai_avg_days decimal(10,2) COMMENT '近期非集采还款平均用时',
    last_month_avg_days decimal(10,2) COMMENT '上月还款用时',
    last_month_jicai_avg_days decimal(10,2) COMMENT '上月集采还款平均用时',
    last_month_nonjicai_avg_days decimal(10,2) COMMENT '上月非集采还款平均用时',
    
    -- 账龄分析指标
    over_1year_unpaid decimal(18,2) COMMENT '超一年应收账款和',
    over_1to2year_unpaid decimal(18,2) COMMENT '超1-2年应收账款和',
    over_2to3year_unpaid decimal(18,2) COMMENT '超2-3年应收账款和',
    over_3to4year_unpaid decimal(18,2) COMMENT '超3-4年应收账款和',
    over_5year_unpaid decimal(18,2) COMMENT '超5年以上应收账款和',
    
    -- 风险指标
    bad_debt_provision decimal(18,2) COMMENT '坏账计提',
    is_key_customer tinyint COMMENT '应收重点客户',
    
    dw_updatetime datetime COMMENT '数仓更新时间'
)
UNIQUE KEY(year_month, entryid, customid)
DISTRIBUTED BY HASH(entryid, customid)
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2"
);

