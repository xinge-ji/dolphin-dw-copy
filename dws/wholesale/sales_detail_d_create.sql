DROP TABLE IF EXISTS dws.wholesale_sales_detail_d;
CREATE TABLE dws.wholesale_sales_detail_d (
    -- 颗粒度
    stat_date date COMMENT '统计日期',
    entryid bigint COMMENT '独立单元ID',
    customid bigint COMMENT '客户ID',
    salerid bigint COMMENT '业务员ID',
    inputmanid bigint COMMENT '录入人ID',
    nianbao_type varchar COMMENT '货品年报分类',
    jicai_type varchar COMMENT '订单集采类型',
    order_source varchar COMMENT '订单来源',
    
    -- 维度
    entry_name varchar COMMENT '独立单元名称',
    province_name varchar COMMENT '省份名称',
    customer_name varchar COMMENT '客户名称',
    customertype_name varchar COMMENT '客户类型',
    customertype_group varchar COMMENT '客户类型组',
    saler_name varchar COMMENT '业务员名称',
    inputman_name varchar COMMENT '录入人名称',
    
    -- 指标
    sales_amount decimal(18,4) COMMENT '销售金额',
    sales_gross_profit decimal(18,4) COMMENT '批次毛利额',
    order_count int COMMENT '总单数',
    order_item_count int COMMENT '细单数',
    ecommerce_sales_amount decimal(18,4) COMMENT '电商销售额',
    non_ecommerce_sales_amount decimal(18,4) COMMENT '非电商销售额',
    ecommerce_order_count int COMMENT '电商总单数',
    non_ecommerce_order_count int COMMENT '非电商总单数',
    return_amount decimal(18,4) COMMENT '销退金额',
    return_order_count int COMMENT '销退总单数'
)
UNIQUE KEY(stat_date,
    entryid,
    customid,
    salerid,
    inputmanid,
    nianbao_type,
    jicai_type,
    order_source)
DISTRIBUTED BY HASH(stat_date, entryid)
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2"
);

INSERT INTO dws.wholesale_sales_detail_d (
    stat_date,
    entryid,
    customid,
    salerid,
    inputmanid,
    nianbao_type,
    jicai_type,
    order_source,
    entry_name,
    province_name,
    customer_name,
    customertype_name,
    customertype_group,
    saler_name,
    inputman_name,
    sales_amount,
    sales_gross_profit,
    order_count,
    order_item_count,
    ecommerce_sales_amount,
    non_ecommerce_sales_amount,
    ecommerce_order_count,
    non_ecommerce_order_count,
    return_amount,
    return_order_count
)
SELECT
    -- 颗粒度
    DATE(wos.create_date) AS stat_date,
    wos.entryid,
    wos.customid,
    wos.salerid AS salesman_id,
    wos.inputmanid AS recorder_id,
    wos.nianbao_type,
    wos.jicai_type,
    wos.comefrom AS order_source,
    
    -- 维度
    wos.entry_name,
    wos.province_name,
    wos.customer_name,
    wos.customertype_name,
    wos.customertype_group,
    wos.saler_name,
    wos.inputman_name,
    
    -- 销售指标
    SUM(CASE WHEN wos.sale_type != '销退' THEN wos.sales_amount ELSE 0 END) AS sales_amount,
    SUM(CASE WHEN wos.sale_type != '销退' THEN wos.sales_gross_profit ELSE 0 END) AS sales_gross_profit,
    COUNT(DISTINCT CASE WHEN wos.sale_type != '销退' THEN wos.salesid END) AS order_count,
    COUNT(CASE WHEN wos.sale_type != '销退' THEN wos.salesdtlid END) AS order_item_count,
    
    -- 电商相关指标
    SUM(CASE 
        WHEN wos.sale_type != '销退' AND wos.is_dianshang = 1 THEN wos.sales_amount 
        ELSE 0 
    END) AS ecommerce_sales_amount,
    
    SUM(CASE 
        WHEN wos.sale_type != '销退' AND wos.is_dianshang = 0 THEN wos.sales_amount 
        ELSE 0 
    END) AS non_ecommerce_sales_amount,
    
    COUNT(DISTINCT CASE 
        WHEN wos.sale_type != '销退' AND wos.is_dianshang = 1 THEN wos.salesid 
    END) AS ecommerce_order_count,
    
    COUNT(DISTINCT CASE 
        WHEN wos.sale_type != '销退' AND wos.is_dianshang = 0 THEN wos.salesid 
    END) AS non_ecommerce_order_count,
    
    -- 销退相关指标
    SUM(CASE 
        WHEN wos.sale_type = '销退' THEN wos.sales_amount 
        ELSE 0 
    END) AS return_amount,
    
    COUNT(DISTINCT CASE 
        WHEN wos.sale_type = '销退' THEN wos.salesid 
    END) AS return_order_count
FROM
    dwd.wholesale_order_sales_dtl wos
WHERE
    wos.use_status = '正式' 
GROUP BY
    DATE(wos.create_date),
    wos.entryid,
    wos.customid,
    wos.salerid,
    wos.inputmanid,
    wos.nianbao_type,
    wos.jicai_type,
    wos.comefrom,
    wos.entry_name,
    wos.province_name,
    wos.customer_name,
    wos.customertype_name,
    wos.customertype_group,
    wos.saler_name,
    wos.inputman_name;


CREATE INDEX IF NOT EXISTS idx_stat_date ON dws.wholesale_sales_detail_d (stat_date);
CREATE INDEX IF NOT EXISTS idx_entryid ON dws.wholesale_sales_detail_d (entryid);
CREATE INDEX IF NOT EXISTS idx_customid ON dws.wholesale_sales_detail_d (customid);
CREATE INDEX IF NOT EXISTS idx_salerid ON dws.wholesale_sales_detail_d (salerid);
CREATE INDEX IF NOT EXISTS idx_inputmanid ON dws.wholesale_sales_detail_d (inputmanid);
CREATE INDEX IF NOT EXISTS idx_order_source ON dws.wholesale_sales_detail_d (order_source);
CREATE INDEX IF NOT EXISTS idx_nianbao_type ON dws.wholesale_sales_detail_d (nianbao_type);
CREATE INDEX IF NOT EXISTS idx_jicai_type ON dws.wholesale_sales_detail_d (jicai_type);
CREATE INDEX IF NOT EXISTS idx_customertype_name ON dws.wholesale_sales_detail_d (customertype_name);
CREATE INDEX IF NOT EXISTS idx_customertype_group ON dws.wholesale_sales_detail_d (customertype_group);
