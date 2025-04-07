DROP TABLE IF EXISTS dws.wholesale_customer_sales_d;
CREATE TABLE dws.wholesale_customer_sales_d (
    -- 颗粒度
    stat_date date COMMENT '统计日期',
    entryid bigint COMMENT '独立单元ID',
    customid bigint COMMENT '客户ID',
    
    -- 维度
    entry_name varchar COMMENT '独立单元名称',
    province_name varchar COMMENT '省份名称',
    customer_name varchar COMMENT '客户名称',
    customertype_name varchar COMMENT '客户类型',
    customertype_group varchar COMMENT '客户类型组',
    
    -- 指标
    sales_amount decimal(18,4) COMMENT '销售金额',
    sales_gross_profit decimal(18,4) COMMENT '批次毛利额',
    order_count int COMMENT '总单数',
    order_item_count int COMMENT '细单数',
    adjustment_amount decimal(18,4) COMMENT '冲差金额',
    adjustment_order_count int COMMENT '冲差总单数'
)
UNIQUE KEY(stat_date, entryid, customid)
DISTRIBUTED BY HASH(stat_date, entryid)
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2"
);

INSERT INTO dws.wholesale_customer_sales_d (
    stat_date,
    entryid,
    customid,
    entry_name,
    province_name,
    customer_name,
    customertype_name,
    customertype_group,
    sales_amount,
    sales_gross_profit,
    order_count,
    order_item_count,
    adjustment_amount,
    adjustment_order_count
)
SELECT
    -- 颗粒度
    DATE(wos.create_date) AS stat_date,
    wos.entryid,
    wos.customid,
    
    -- 维度
    wos.entry_name,
    wos.province_name,
    wos.customer_name,
    wos.customertype_name,
    wos.customertype_group,
    
    -- 销售指标
    SUM(CASE WHEN wos.sale_type != '销退' THEN wos.sales_amount ELSE 0 END) AS sales_amount,
    SUM(CASE WHEN wos.sale_type != '销退' THEN wos.sales_gross_profit ELSE 0 END) AS sales_gross_profit,
    COUNT(DISTINCT CASE WHEN wos.sale_type != '销退' THEN wos.salesid END) AS total_order_count,
    COUNT(CASE WHEN wos.sale_type != '销退' THEN wos.salesdtlid END) AS detail_order_count,
    
    -- 冲差相关指标
    SUM(CASE 
        WHEN wos.sale_type = '销退' AND wos.comefrom = '手工录入' THEN wos.sales_amount 
        ELSE 0 
    END) AS adjustment_amount,
    
    COUNT(DISTINCT CASE 
        WHEN wos.sale_type = '销退' AND wos.comefrom = '手工录入' THEN wos.salesid 
    END) AS adjustment_order_count
FROM
    dwd.wholesale_order_sales_dtl wos
WHERE
    wos.use_status = '正式' 
GROUP BY
    DATE(wos.create_date),
    wos.entryid,
    wos.customid,
    wos.entry_name,
    wos.province_name,
    wos.customer_name,
    wos.customertype_name,
    wos.customertype_group;

CREATE INDEX IF NOT EXISTS idx_stat_date ON dws.wholesale_customer_sales_d (stat_date);
CREATE INDEX IF NOT EXISTS idx_entryid ON dws.wholesale_customer_sales_d (entryid);
CREATE INDEX IF NOT EXISTS idx_customid ON dws.wholesale_customer_sales_d (customid);
CREATE INDEX IF NOT EXISTS idx_customertype_name ON dws.wholesale_customer_sales_d (customertype_name);
CREATE INDEX IF NOT EXISTS idx_customertype_group ON dws.wholesale_customer_sales_d (customertype_group);
