DROP TABLE IF EXISTS dws.wholesale_settle_detail_d;
CREATE TABLE dws.wholesale_settle_detail_d (
    -- 颗粒度
    stat_date date COMMENT '统计日期',
    entryid bigint COMMENT '独立单元ID',
    customid bigint COMMENT '客户ID',
    nianbao_type varchar COMMENT '货品年报分类',
    jicai_type varchar COMMENT '订单集采类型',
    order_source varchar COMMENT '订单来源',
    
    -- 维度
    entry_name varchar COMMENT '独立单元名称',
    province_name varchar COMMENT '省份名称',
    customer_name varchar COMMENT '客户名称',
    customertype_name varchar COMMENT '客户类型名称',
    customertype_group varchar COMMENT '客户类型组',
    
    -- 指标
    settle_amount decimal(18,4) COMMENT '结算金额',
    settle_count int COMMENT '结算单数',
    settle_item_count int COMMENT '结算明细单数',
    settle_order_count int COMMENT '订单结算完成单数',
    avg_order_settle_time decimal(10,4) COMMENT '平均销售结算用时(天)',
    settle_order_item_count int COMMENT '订单明细完成单数',
    avg_order_item_settle_time decimal(10,4) COMMENT '平均销售明细结算用时(天)'
)
UNIQUE KEY(stat_date, entryid, customid, nianbao_type, jicai_type, order_source)
DISTRIBUTED BY HASH(stat_date, entryid)
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2"
);

-- 插入结算汇总数据
INSERT INTO dws.wholesale_settle_detail_d (
    stat_date,
    entryid,
    customid,
    nianbao_type,
    jicai_type,
    order_source,
    entry_name,
    province_name,
    customer_name,
    customertype_name,
    customertype_group,
    settle_amount,
    settle_count,
    settle_item_count,
    settle_order_count,
    avg_order_settle_time,
    settle_rder_item_count,
    avg_order_item_settle_time
)
WITH settle_summary AS (
    -- 先按照分组维度聚合结算明细数据
    SELECT
        DATE(osd.confirm_date) AS stat_date,
        osd.entryid,
        osd.customid,
        IFNULL(osd.nianbao_type, 'UNKNOWN') AS nianbao_type,
        osd.jicai_type,
        osd.comefrom AS order_source,
        osd.entry_name,
        osd.province_name,
        osd.customer_name,
        osd.customertype_name,
        osd.customertype_group,
        SUM(osd.settle_amount) AS settle_amount,
        COUNT(DISTINCT osd.sasettleid) AS settle_count,
        COUNT(DISTINCT osd.sasettledtlid) AS settle_item_count
    FROM
        dwd.wholesale_order_settle_dtl osd
    WHERE
        osd.use_status = '正式'
        AND osd.confirm_date IS NOT NULL
    GROUP BY
        DATE(osd.confirm_date),
        osd.entryid,
        osd.customid,
        IFNULL(osd.nianbao_type, 'UNKNOWN'),
        osd.jicai_type,
        osd.comefrom,
        osd.entry_name,
        osd.province_name,
        osd.customer_name,
        osd.customertype_name,
        osd.customertype_group
),
order_settlement AS (
    -- 计算每个销售单的结算用时
    SELECT 
        DATE(ss.order_settle_time) as stat_date,
        ss.entryid,
        ss.customid,
        IFNULL(ss.nianbao_type, 'UNKNOWN') AS nianbao_type,
        ss.jicai_type,
        ss.comefrom AS order_source,
        ss.entry_name,
        ss.province_name,
        ss.customer_name,
        ss.customertype_name,
        ss.customertype_group,
        SUM(ss.order_settle_status) AS settle_order_count,
        ROUND(SUM(datediff(ss.order_settle_time, ss.yewu_date))/SUM(ss.order_settle_status),4) AS avg_order_settle_time
    FROM
        (select distinct salesid, order_settle_time,
        entryid, customid, nianbao_type, jicai_type,comefrom,entry_name, province_name,customer_name,customertype_name,customertype_group,
        order_settle_status, yewu_date FROM dwd.wholesale_sales_ar_detail
        WHERE order_settle_time IS NOT NULL) ss
    GROUP BY
        DATE(ss.order_settle_time),
        ss.entryid,
        ss.customid,
        IFNULL(ss.nianbao_type, 'UNKNOWN'),
        ss.jicai_type,
        ss.comefrom,
        ss.entry_name,
        ss.province_name,
        ss.customer_name,
        ss.customertype_name,
        ss.customertype_group
),
order_item_settlement AS (
    -- 计算每个销售单明细的结算用时
    SELECT 
        DATE(ss.order_item_settle_time) as stat_date,
        ss.entryid,
        ss.customid,
        IFNULL(ss.nianbao_type, 'UNKNOWN') AS nianbao_type,
        ss.jicai_type,
        ss.comefrom AS order_source,
        ss.entry_name,
        ss.province_name,
        ss.customer_name,
        ss.customertype_name,
        ss.customertype_group,
        SUM(ss.order_item_settle_status) AS settle_order_item_count,
        ROUND(SUM(datediff(ss.order_item_settle_time, ss.yewu_date))/SUM(ss.order_item_settle_status),4) AS avg_order_item_settle_time
    FROM
        dwd.wholesale_sales_ar_detail ss
    WHERE 
        ss.order_item_settle_time IS NOT NULL
    GROUP BY
        DATE(ss.order_item_settle_time),
        ss.entryid,
        ss.customid,
        IFNULL(ss.nianbao_type, 'UNKNOWN'),
        ss.jicai_type,
        ss.comefrom,
        ss.entry_name,
        ss.province_name,
        ss.customer_name,
        ss.customertype_name,
        ss.customertype_group
)
SELECT
    ss.stat_date,
    ss.entryid,
    ss.customid,
    ss.nianbao_type,
    ss.jicai_type,
    ss.order_source,
    ss.entry_name,
    ss.province_name,
    ss.customer_name,
    ss.customertype_name,
    ss.customertype_group,
    ss.settle_amount,
    ss.settle_count,
    ss.settle_item_count,
    COALESCE(os.settle_order_count, 0) AS settle_order_count,
    COALESCE(os.avg_order_settle_time, 0) AS avg_order_settle_time,
    COALESCE(ois.settle_order_item_count, 0) AS settle_order_item_count,
    COALESCE(ois.avg_order_item_settle_time, 0) AS avg_order_item_settle_time
FROM settle_summary ss
LEFT JOIN order_settlement os ON 
    ss.stat_date = os.stat_date AND
    ss.entryid = os.entryid AND
    ss.customid = os.customid AND
    ss.nianbao_type = os.nianbao_type AND
    ss.jicai_type = os.jicai_type AND
    ss.order_source = os.order_source
LEFT JOIN order_item_settlement ois ON 
    ss.stat_date = ois.stat_date AND
    ss.entryid = ois.entryid AND
    ss.customid = ois.customid AND
    ss.nianbao_type = ois.nianbao_type AND
    ss.jicai_type = ois.jicai_type AND
    ss.order_source = ois.order_source;

CREATE INDEX IF NOT EXISTS idx_stat_date ON dws.wholesale_settle_detail_d (stat_date);
CREATE INDEX IF NOT EXISTS idx_entryid ON dws.wholesale_settle_detail_d (entryid);
CREATE INDEX IF NOT EXISTS idx_customid ON dws.wholesale_settle_detail_d (customid);
CREATE INDEX IF NOT EXISTS idx_nianbao_type ON dws.wholesale_settle_detail_d (nianbao_type);
CREATE INDEX IF NOT EXISTS idx_jicai_type ON dws.wholesale_settle_detail_d (jicai_type);
CREATE INDEX IF NOT EXISTS idx_order_source ON dws.wholesale_settle_detail_d (order_source);
CREATE INDEX IF NOT EXISTS idx_province_name ON dws.wholesale_settle_detail_d (province_name);
CREATE INDEX IF NOT EXISTS idx_customertype_group ON dws.wholesale_settle_detail_d (customertype_group);