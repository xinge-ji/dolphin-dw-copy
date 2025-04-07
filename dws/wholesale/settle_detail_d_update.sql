DELETE FROM dws.wholesale_sales_detail_d
where stat_date > CURRENT_DATE() - INTERVAL 60 DAY;

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
    settle_order_item_count,
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
        AND osd.confirm_date >= DATE_SUB(NOW(), INTERVAL 60 DAY) 
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
        (select distinct sr.salesid, sr.order_settle_time,
        sr.entryid, sr.customid, sod.nianbao_type, sr.jicai_type,sr.comefrom,sr.entry_name, sr.province_name,sr.customer_name,
        sr.customertype_name,sr.customertype_group,sr.order_settle_status, sr.yewu_date 
        FROM dwd.wholesale_sales_receivable_detail sr
        LEFT JOIN dwd.wholesale_order_sales_dtl sod ON sr.salesid = sod.salesid
        WHERE sr.order_settle_time IS NOT NULL
        AND sr.order_settle_time >= DATE_SUB(NOW(), INTERVAL 60 DAY)) ss
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
        IFNULL(sod.nianbao_type, 'UNKNOWN') AS nianbao_type,
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
        dwd.wholesale_sales_receivable_detail ss
    LEFT JOIN dwd.wholesale_order_sales_dtl sod ON ss.salesid = sod.salesid
    WHERE 
        ss.order_item_settle_time IS NOT NULL
        AND ss.order_item_settle_time >= DATE_SUB(NOW(), INTERVAL 60 DAY)
    GROUP BY
        DATE(ss.order_item_settle_time),
        ss.entryid,
        ss.customid,
        IFNULL(sod.nianbao_type, 'UNKNOWN'),
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