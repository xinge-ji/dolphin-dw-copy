DELETE FROM dws.wholesale_repay_detail_d
where stat_date > CURRENT_DATE() - INTERVAL 60 DAY;

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
    repaid_count,
    repaid_item_count,
    repaid_order_count,
    avg_order_repaid_time,
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
        COUNT(DISTINCT r.repayid) AS repaid_count,
        COUNT(DISTINCT r.repaydtlid) AS repaid_item_count
    FROM
        dwd.wholesale_order_repay_dtl r
    LEFT JOIN
        dwd.wholesale_order_sales_dtl s ON r.salesid = s.salesid
    WHERE
        r.use_status = '正式'
        AND r.payment_date IS NOT NULL
        AND r.payment_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
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
order_payment AS (
    -- 计算每个销售单的回款用时
    SELECT 
        DATE(ss.order_payment_time) as stat_date,
        ss.entryid,
        ss.customid,
        IFNULL(ss.nianbao_type, 'UNKNOWN') AS nianbao_type,
        ss.jicai_type,
        ss.entry_name,
        ss.province_name,
        ss.customer_name,
        ss.customertype_name,
        ss.customertype_group,
        SUM(ss.order_payment_status) AS repaid_order_count,
        ROUND(SUM(datediff(ss.order_payment_time, ss.yewu_date))/SUM(ss.order_payment_status),4) AS avg_order_repaid_time
    FROM
        (select distinct sr.salesid, sr.order_settle_time,
        sr.entryid, sr.customid, sod.nianbao_type, sr.jicai_type,sr.comefrom,sr.entry_name, sr.province_name,sr.customer_name,
        sr.customertype_name,sr.customertype_group,sr.order_settle_status, sr.yewu_date 
        FROM dwd.wholesale_sales_receivable_detail sr
        LEFT JOIN dwd.wholesale_order_sales_dtl sod ON sr.salesid = sod.salesid
        WHERE sr.order_payment_time IS NOT NULL
        AND sr.order_payment_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)) ss
    GROUP BY
        DATE(ss.order_payment_time),
        ss.entryid,
        ss.customid,
        IFNULL(ss.nianbao_type, 'UNKNOWN'),
        ss.jicai_type,
        ss.entry_name,
        ss.province_name,
        ss.customer_name,
        ss.customertype_name,
        ss.customertype_group
),
order_item_payment AS (
    -- 计算每个销售单明细的回款用时
    SELECT 
        DATE(ss.order_item_payment_time) as stat_date,
        ss.entryid,
        ss.customid,
        IFNULL(sod.nianbao_type, 'UNKNOWN') AS nianbao_type,
        ss.jicai_type,
        ss.entry_name,
        ss.province_name,
        ss.customer_name,
        ss.customertype_name,
        ss.customertype_group,
        SUM(ss.order_item_payment_status) AS repaid_order_item_count,
        ROUND(SUM(datediff(ss.order_item_payment_time, ss.yewu_date))/SUM(ss.order_item_payment_status),4) AS avg_order_item_repaid_time
    FROM
        dwd.wholesale_sales_receivable_detail ss
    LEFT JOIN dwd.wholesale_order_sales_dtl sod ON ss.salesid = sod.salesid
    WHERE 
        ss.order_item_payment_time IS NOT NULL
        AND ss.order_item_payment_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    GROUP BY
        DATE(ss.order_item_payment_time),
        ss.entryid,
        ss.customid,
        IFNULL(sod.nianbao_type, 'UNKNOWN'),
        ss.jicai_type,
        ss.entry_name,
        ss.province_name,
        ss.customer_name,
        ss.customertype_name,
        ss.customertype_group
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
    rs.repaid_count,
    rs.repaid_item_count,
    COALESCE(op.repaid_order_count, 0) AS repaid_order_count,
    COALESCE(op.avg_order_repaid_time, 0) AS avg_order_repaid_time,
    COALESCE(oip.repaid_order_item_count, 0) AS repaid_order_item_count,
    COALESCE(oip.avg_order_item_repaid_time, 0) AS avg_order_item_repaid_time
FROM repay_summary rs
LEFT JOIN order_payment op ON 
    rs.stat_date = op.stat_date AND
    rs.entryid = op.entryid AND
    rs.customid = op.customid AND
    rs.nianbao_type = op.nianbao_type AND
    rs.jicai_type = op.jicai_type
LEFT JOIN order_item_payment oip ON 
    rs.stat_date = oip.stat_date AND
    rs.entryid = oip.entryid AND
    rs.customid = oip.customid AND
    rs.nianbao_type = oip.nianbao_type AND
    rs.jicai_type = oip.jicai_type;