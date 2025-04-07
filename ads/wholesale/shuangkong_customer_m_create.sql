DROP TABLE IF EXISTS ads.shuangkong_customer_m;
CREATE TABLE ads.shuangkong_customer_m (
    -- 颗粒度
    stat_yearmonth DATE COMMENT "业务年月",
    entryid bigint COMMENT '独立单元ID',
    customid bigint COMMENT '客户ID',
    jicai_type VARCHAR(100) COMMENT '集采类型',
    nianbao_type VARCHAR(100) COMMENT '年报类型',

    -- 维度
    entry_name VARCHAR(255) COMMENT '独立单元名称',
    province_name VARCHAR(255) COMMENT '省份名称',
    city_name VARCHAR(255) COMMENT '城市名称',
    customer_name VARCHAR(255) COMMENT '客户名称',
    customertype int comment '客户类型',
    customertype_name varchar comment '客户类型名称',
    is_shuangwanjia tinyint comment '是否双万家',
    customertype_group varchar comment '客户类型组'

    -- 本月指标
    current_sales_amount decimal(18,4) COMMENT '本月销售额',
    current_sales_gross_profit decimal(18,4) COMMENT '本月毛利额',
    current_settle_amount decimal(18,4) COMMENT '本月结算金额',
    current_repaid_amount decimal(18,4) COMMENT '本月回款金额',

    -- 上月指标
    prev_sales_amount decimal(18,4) COMMENT '上月销售额',
    prev_sales_gross_profit decimal(18,4) COMMENT '上月毛利额',
    prev_settle_amount decimal(18,4) COMMENT '上月结算金额',
    prev_repaid_amount decimal(18,4) COMMENT '上月回款金额',

    -- 近三个月指标
    avg_3m_sales_amount decimal(18,4) COMMENT '近三个月销售额',
    avg_3m_sales_gross_profit decimal(18,4) COMMENT '近三个月毛利额',
    avg_3m_settle_amount decimal(18,4) COMMENT '近三个月结算金额',
    avg_3m_repaid_amount decimal(18,4) COMMENT '近三个月回款金额'
)
UNIQUE KEY(stat_yearmonth, entryid, customid, jicai_type, nianbao_type)
DISTRIBUTED BY HASH(stat_yearmonth, entryid)
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2"
);