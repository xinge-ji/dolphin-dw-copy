DROP TABLE IF EXISTS dws.wholesale_repay_detail_d;
CREATE TABLE dws.wholesale_repay_detail_d (
    -- 颗粒度
    stat_date date COMMENT '统计日期',
    entryid bigint COMMENT '独立单元ID',
    customid bigint COMMENT '客户ID',
    nianbao_type varchar COMMENT '货品年报分类',
    jicai_type varchar COMMENT '订单集采类型',
    
    -- 维度
    entry_name varchar COMMENT '独立单元名称',
    province_name varchar COMMENT '省份名称',
    customer_name varchar COMMENT '客户名称',
    customertype_name varchar COMMENT '客户类型',
    customertype_group varchar COMMENT '客户类型组',
    
    -- 回款指标
    repaid_amount decimal(18,4) COMMENT '当日回款金额',
    repaid_item_count int COMMENT '回款明细单数',
    repaid_order_item_count int COMMENT '订单明细回款完成单数',
    avg_order_item_repaid_time decimal(10,4) COMMENT '平均销售明细回款用时(天)'
)
UNIQUE KEY(stat_date, entryid, customid, nianbao_type, jicai_type)
DISTRIBUTED BY HASH(stat_date, entryid)
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2"
);

INSERT INTO dws.wholesale_repay_detail_d (
    stat_date,
    entryid,
    customid,
    nianbao_type,
    jicai_type,
    entry_name,
    province_name,
    customer_name,
    customertype_name,
    customertype_group,
    repaid_amount,
    repaid_item_count,
    repaid_order_item_count,
    avg_order_item_repaid_time
)
WITH repay_summary AS (
    -- 按照分组维度聚合回款明细数据
    SELECT
        DATE(r.payment_date) AS stat_date,
        r.entryid,
        r.customid,
        IFNULL(r.nianbao_type, 'UNKNOWN') AS nianbao_type,
        IFNULL(r.jicai_type, 'UNKNOWN') AS jicai_type,
        r.entry_name,
        r.province_name,
        r.customer_name,
        COALESCE(s.customertype_name, 'UNKNOWN') AS customertype_name,
        COALESCE(s.customertype_group, 'UNKNOWN') AS customertype_group,
        SUM(r.payment_amount) AS repaid_amount,
        COUNT(DISTINCT r.sarecid) AS repaid_count,
        COUNT(DISTINCT r.sarecdtlid) AS repaid_item_count
    FROM
        dwd.wholesale_order_repay_dtl r
    LEFT JOIN
        dwd.wholesale_order_sales_dtl s ON r.salesid = s.salesid
    WHERE
        r.use_status = '正式'
        AND r.payment_date IS NOT NULL
    GROUP BY
        DATE(r.payment_date),
        r.entryid,
        r.customid,
        IFNULL(r.nianbao_type, 'UNKNOWN'),
        IFNULL(r.jicai_type, 'UNKNOWN'),
        r.entry_name,
        r.province_name,
        r.customer_name,
        COALESCE(s.customertype_name, 'UNKNOWN'),
        COALESCE(s.customertype_group, 'UNKNOWN')
),
order_item_payment AS (
    -- 计算每个销售单明细的回款用时
    SELECT 
        DATE(ss.received_time) as stat_date,
        sod.entryid,
        sod.customid,
        IFNULL(sod.nianbao_type, 'UNKNOWN') AS nianbao_type,
        sod.jicai_type,
        sod.entry_name,
        sod.province_name,
        sod.customer_name,
        sod.customertype_name,
        sod.customertype_group,
        SUM(ss.is_received) AS repaid_order_item_count,
        ROUND(SUM(datediff(ss.received_time, ss.yewu_date))/SUM(ss.is_received),4) AS avg_order_item_repaid_time
    FROM
        dwd.wholesale_sales_receivable_dtl ss
    LEFT JOIN dwd.wholesale_order_sales_dtl sod ON ss.salesid = sod.salesid
    WHERE 
        ss.received_time IS NOT NULL
    GROUP BY
        DATE(ss.received_time),
        sod.entryid,
        sod.customid,
        IFNULL(sod.nianbao_type, 'UNKNOWN'),
        sod.jicai_type,
        sod.entry_name,
        sod.province_name,
        sod.customer_name,
        sod.customertype_name,
        sod.customertype_group
)
SELECT
    rs.stat_date,
    rs.entryid,
    rs.customid,
    rs.nianbao_type,
    rs.jicai_type,
    rs.entry_name,
    rs.province_name,
    rs.customer_name,
    rs.customertype_name,
    rs.customertype_group,
    rs.repaid_amount,
    rs.repaid_item_count,
    COALESCE(oip.repaid_order_item_count, 0) AS repaid_order_item_count,
    COALESCE(oip.avg_order_item_repaid_time, 0) AS avg_order_item_repaid_time
FROM repay_summary rs
LEFT JOIN order_item_payment oip ON 
    rs.stat_date = oip.stat_date AND
    rs.entryid = oip.entryid AND
    rs.customid = oip.customid AND
    rs.nianbao_type = oip.nianbao_type AND
    rs.jicai_type = oip.jicai_type;

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_stat_date ON dws.wholesale_repay_detail_d (stat_date);
CREATE INDEX IF NOT EXISTS idx_entryid ON dws.wholesale_repay_detail_d (entryid);
CREATE INDEX IF NOT EXISTS idx_customid ON dws.wholesale_repay_detail_d (customid);
CREATE INDEX IF NOT EXISTS idx_nianbao_type ON dws.wholesale_repay_detail_d (nianbao_type);
CREATE INDEX IF NOT EXISTS idx_jicai_type ON dws.wholesale_repay_detail_d (jicai_type);
CREATE INDEX IF NOT EXISTS idx_province_name ON dws.wholesale_repay_detail_d (province_name);
CREATE INDEX IF NOT EXISTS idx_customertype_name ON dws.wholesale_repay_detail_d (customertype_name);
CREATE INDEX IF NOT EXISTS idx_customertype_group ON dws.wholesale_repay_detail_d (customertype_group);