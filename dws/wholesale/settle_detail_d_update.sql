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
    settle_item_count,
    settled_order_item_count,
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
order_item_settlement AS (
    -- 计算每个销售单明细的结算用时
    SELECT 
        DATE(ss.settle_time) as stat_date,
        sod.entryid,
        sod.customid,
        IFNULL(sod.nianbao_type, 'UNKNOWN') AS nianbao_type,
        sod.jicai_type,
        sod.comefrom AS order_source,
        sod.entry_name,
        sod.province_name,
        sod.customer_name,
        sod.customertype_name,
        sod.customertype_group,
        SUM(ss.is_settled) AS settled_order_item_count,
        ROUND(SUM(datediff(ss.settle_time, ss.yewu_date))/SUM(ss.is_settled),4) AS avg_order_item_settle_time
    FROM
        dwd.wholesale_sales_receivable_dtl ss
    LEFT JOIN dwd.wholesale_order_sales_dtl sod ON ss.salesid = sod.salesid
    WHERE 
        ss.settle_time IS NOT NULL
        AND ss.settle_time >= DATE_SUB(NOW(), INTERVAL 60 DAY)
    GROUP BY
        DATE(ss.settle_time),
        sod.entryid,
        sod.customid,
        IFNULL(sod.nianbao_type, 'UNKNOWN'),
        sod.jicai_type,
        sod.comefrom,
        sod.entry_name,
        sod.province_name,
        sod.customer_name,
        sod.customertype_name,
        sod.customertype_group
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
    ss.settle_item_count,
    COALESCE(ois.settled_order_item_count, 0) AS settled_order_item_count,
    COALESCE(ois.avg_order_item_settle_time, 0) AS avg_order_item_settle_time
FROM settle_summary ss
LEFT JOIN order_item_settlement ois ON 
    ss.stat_date = ois.stat_date AND
    ss.entryid = ois.entryid AND
    ss.customid = ois.customid AND
    ss.nianbao_type = ois.nianbao_type AND
    ss.jicai_type = ois.jicai_type AND
    ss.order_source = ois.order_source;