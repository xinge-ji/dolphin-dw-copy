DELETE FROM dws.wholesale_customer_sales_d
WHERE stat_date > CURRENT_DATE() - INTERVAL 60 DAY;

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
    adjustment_order_count,
    settle_amount,
    settle_count,
    settled_order_count,
    avg_order_settle_time,
    settle_item_count,
    settled_order_item_count,
    avg_order_item_settle_time,
    repaid_amount,
    repaid_count,
    repaid_order_count,
    avg_order_repaid_time,
    repaid_item_count,
    repaid_order_item_count,
    avg_order_item_repaid_time
)
WITH sales_summary AS (
    -- 销售数据汇总
    SELECT
        DATE(wos.create_date) AS stat_date,
        wos.entryid,
        wos.customid,
        wos.entry_name,
        wos.province_name,
        wos.customer_name,
        wos.customertype_name,
        wos.customertype_group,
        SUM(CASE WHEN wos.sale_type != '销退' THEN wos.sales_amount ELSE 0 END) AS sales_amount,
        SUM(CASE WHEN wos.sale_type != '销退' THEN wos.sales_gross_profit ELSE 0 END) AS sales_gross_profit,
        COUNT(DISTINCT CASE WHEN wos.sale_type != '销退' THEN wos.salesid END) AS order_count,
        COUNT(CASE WHEN wos.sale_type != '销退' THEN wos.salesdtlid END) AS order_item_count,
        SUM(CASE WHEN wos.sale_type = '销退' AND wos.comefrom = '手工录入' THEN wos.sales_amount ELSE 0 END) AS adjustment_amount,
        COUNT(DISTINCT CASE WHEN wos.sale_type = '销退' AND wos.comefrom = '手工录入' THEN wos.salesid END) AS adjustment_order_count
    FROM
        dwd.wholesale_order_sales_dtl wos
    WHERE
        wos.use_status = '正式' 
        AND date(wos.create_date) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND date(wos.create_date) < CURRENT_DATE()
    GROUP BY
        DATE(wos.create_date),
        wos.entryid,
        wos.customid,
        wos.entry_name,
        wos.province_name,
        wos.customer_name,
        wos.customertype_name,
        wos.customertype_group
),
settle_summary AS (
    -- 结算数据汇总
    SELECT
        DATE(osd.confirm_date) AS stat_date,
        osd.entryid,
        osd.customid,
        SUM(osd.settle_amount) AS settle_amount,
        COUNT(DISTINCT osd.sasettleid) AS settle_count,
        COUNT(DISTINCT osd.sasettledtlid) AS settle_item_count
    FROM
        dwd.wholesale_order_settle_dtl osd
    WHERE
        osd.use_status = '正式'
        AND osd.confirm_date IS NOT NULL
        AND date(osd.confirm_date) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND date(osd.confirm_date) < CURRENT_DATE()
    GROUP BY
        DATE(osd.confirm_date),
        osd.entryid,
        osd.customid
),
order_settlement AS (
    -- 销售单结算用时
    SELECT 
        DATE(ss.settle_time) as stat_date,
        sod.entryid,
        sod.customid,
        SUM(ss.is_settled) AS settled_order_count,
        ROUND(SUM(datediff(ss.settle_time, ss.yewu_date))/SUM(ss.is_settled),4) AS avg_order_settle_time
    FROM
        dwd.wholesale_sales_receivable_doc ss
    LEFT JOIN dwd.wholesale_order_sales_doc sod ON ss.salesid = sod.salesid
    WHERE 
        ss.settle_time IS NOT NULL
        AND date(ss.settle_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND date(ss.settle_time) < CURRENT_DATE()
    GROUP BY
        DATE(ss.settle_time),
        sod.entryid,
        sod.customid
),
order_item_settlement AS (
    -- 销售单明细结算用时
    SELECT 
        DATE(ss.settle_time) as stat_date,
        sod.entryid,
        sod.customid,
        SUM(ss.is_settled) AS settled_order_item_count,
        ROUND(SUM(datediff(ss.settle_time, ss.yewu_date))/SUM(ss.is_settled),4) AS avg_order_item_settle_time
    FROM
        dwd.wholesale_sales_receivable_dtl ss
    LEFT JOIN dwd.wholesale_order_sales_dtl sod ON ss.salesdtlid = sod.salesdtlid
    WHERE 
        ss.settle_time IS NOT NULL
        AND date(ss.settle_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND date(ss.settle_time) < CURRENT_DATE()
    GROUP BY
        DATE(ss.settle_time),
        sod.entryid,
        sod.customid
),
repay_summary AS (
    -- 回款数据汇总
    SELECT
        DATE(r.payment_date) AS stat_date,
        r.entryid,
        r.customid,
        SUM(r.payment_amount) AS repaid_amount,
        COUNT(DISTINCT r.sarecid) AS repaid_count,
        COUNT(DISTINCT r.sarecdtlid) AS repaid_item_count
    FROM
        dwd.wholesale_order_repay_dtl r
    WHERE
        r.use_status = '正式'
        AND r.payment_date IS NOT NULL
        AND date(r.payment_date) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND date(r.payment_date) < CURRENT_DATE()
    GROUP BY
        DATE(r.payment_date),
        r.entryid,
        r.customid
),
order_payment AS (
    -- 销售单回款用时
    SELECT 
        DATE(ss.received_time) as stat_date,
        sod.entryid,
        sod.customid,
        SUM(ss.is_received) AS repaid_order_count,
        ROUND(SUM(datediff(ss.received_time, ss.yewu_date))/SUM(ss.is_received),4) AS avg_order_repaid_time
    FROM
        dwd.wholesale_sales_receivable_doc ss
    LEFT JOIN dwd.wholesale_order_sales_doc sod ON ss.salesid = sod.salesid
    WHERE 
        ss.received_time IS NOT NULL
        AND date(ss.received_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND date(ss.received_time) < CURRENT_DATE()
    GROUP BY
        DATE(ss.received_time),
        sod.entryid,
        sod.customid
),
order_item_payment AS (
    -- 销售单明细回款用时
    SELECT 
        DATE(ss.received_time) as stat_date,
        sod.entryid,
        sod.customid,
        SUM(ss.is_received) AS repaid_order_item_count,
        ROUND(SUM(datediff(ss.received_time, ss.yewu_date))/SUM(ss.is_received),4) AS avg_order_item_repaid_time
    FROM
        dwd.wholesale_sales_receivable_dtl ss
    LEFT JOIN dwd.wholesale_order_sales_dtl sod ON ss.salesdtlid = sod.salesdtlid
    WHERE 
        ss.received_time IS NOT NULL
        AND date(ss.received_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND date(ss.received_time) < CURRENT_DATE()
    GROUP BY
        DATE(ss.received_time),
        sod.entryid,
        sod.customid
)
SELECT
    ss.stat_date,
    ss.entryid,
    ss.customid,
    ss.entry_name,
    ss.province_name,
    ss.customer_name,
    ss.customertype_name,
    ss.customertype_group,
    ss.sales_amount,
    ss.sales_gross_profit,
    ss.order_count,
    ss.order_item_count,
    ss.adjustment_amount,
    ss.adjustment_order_count,
    COALESCE(st.settle_amount, 0) AS settle_amount,
    COALESCE(st.settle_count, 0) AS settle_count,
    COALESCE(os.settled_order_count, 0) AS settled_order_count,
    COALESCE(os.avg_order_settle_time, 0) AS avg_order_settle_time,
    COALESCE(st.settle_item_count, 0) AS settle_item_count,
    COALESCE(ois.settled_order_item_count, 0) AS settled_order_item_count,
    COALESCE(ois.avg_order_item_settle_time, 0) AS avg_order_item_settle_time,
    COALESCE(rs.repaid_amount, 0) AS repaid_amount,
    COALESCE(rs.repaid_count, 0) AS repaid_count,
    COALESCE(op.repaid_order_count, 0) AS repaid_order_count,
    COALESCE(op.avg_order_repaid_time, 0) AS avg_order_repaid_time,
    COALESCE(rs.repaid_item_count, 0) AS repaid_item_count,
    COALESCE(oip.repaid_order_item_count, 0) AS repaid_order_item_count,
    COALESCE(oip.avg_order_item_repaid_time, 0) AS avg_order_item_repaid_time
FROM sales_summary ss
LEFT JOIN settle_summary st ON 
    ss.stat_date = st.stat_date AND
    ss.entryid = st.entryid AND
    ss.customid = st.customid
LEFT JOIN order_settlement os ON 
    ss.stat_date = os.stat_date AND
    ss.entryid = os.entryid AND
    ss.customid = os.customid
LEFT JOIN order_item_settlement ois ON 
    ss.stat_date = ois.stat_date AND
    ss.entryid = ois.entryid AND
    ss.customid = ois.customid
LEFT JOIN repay_summary rs ON 
    ss.stat_date = rs.stat_date AND
    ss.entryid = rs.entryid AND
    ss.customid = rs.customid
LEFT JOIN order_payment op ON 
    ss.stat_date = op.stat_date AND
    ss.entryid = op.entryid AND
    ss.customid = op.customid
LEFT JOIN order_item_payment oip ON 
    ss.stat_date = oip.stat_date AND
    ss.entryid = oip.entryid AND
    ss.customid = oip.customid;