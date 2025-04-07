DELETE FROM dws.eshop_sales_customer_d
WHERE stat_date > CURRENT_DATE() - INTERVAL 60 DAY;

-- 重新插入最近60天的数据
INSERT INTO dws.eshop_sales_customer_d (
    stat_date,
    entryid,
    customid,
    entry_name,
    customer_name,
    order_count,
    sales_amount,
    potential_b2b_order_count,
    potential_b2b_sales_amount,
    b2b_order_count,
    b2b_sales_amount,
    b2b_self_initiated_order_count,
    b2b_self_initiated_sales_amount
)
WITH date_range AS (
    SELECT 
        CURRENT_DATE() - INTERVAL 60 DAY AS start_date,
        CURRENT_DATE() - INTERVAL 1 DAY AS end_date
),
-- 计算销售总单信息
sales_summary AS (
    SELECT
        DATE(wos.create_date) AS stat_date,
        wos.entryid,
        wos.customid,
        COUNT(DISTINCT wos.salesid) AS order_count,
        SUM(wos.sales_amount) AS sales_amount
    FROM dwd.wholesale_order_sales_dtl wos
    CROSS JOIN date_range dr
    WHERE wos.use_status = '正式'
    AND sale_type = '销售'
    AND IFNULL(is_haixi,0) = 0
    AND DATE(wos.create_date) >= dr.start_date AND DATE(wos.create_date) < dr.end_date
    GROUP BY DATE(wos.create_date), wos.entryid, wos.customid 
),
-- 计算可转化为b2b的手工订单信息
potential_b2b AS (
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.customid,
        COUNT(DISTINCT wosd.salesid) AS potential_b2b_order_count,
        SUM(wosd.sales_amount) AS potential_b2b_sales_amount
    FROM dwd.wholesale_order_sales_dtl wosd
    JOIN dim.eshop_entry_goods eeg ON wosd.entryid = eeg.entryid 
        AND wosd.goodsid = eeg.goodsid
        AND wosd.create_date >= eeg.dw_starttime AND wosd.create_date < eeg.dw_endtime
    JOIN dim.eshop_entry_customer ecb ON wosd.customid = ecb.customid
        AND wosd.entryid = ecb.entryid
        AND wosd.create_date >= ecb.dw_starttime AND wosd.create_date < ecb.dw_endtime
    WHERE wosd.comefrom = '手工录入'
        AND (wosd.storageid IS NULL 
             OR wosd.storage_name in ('三明鹭燕合格保管帐','漳州鹭燕大库保管帐','泉州鹭燕大库保管帐','莆田鹭燕大库保管帐','福州鹭燕大库保管帐','宁德鹭燕大库保管帐','龙岩新鹭燕大库保管帐','南平鹭燕大库保管帐','股份厦门大库保管帐')
             )
        AND wosd.use_status = '正式'
        AND wosd.sale_type = '销售'
        AND wosd.is_dianshang = 0
        AND wosd.discount >= 1
        AND DATE(wosd.create_date) >= dr.start_date AND DATE(wosd.create_date) < dr.end_date
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.customid
),
-- 计算b2b订单信息
b2b_orders AS (
    SELECT 
        DATE(eosd.create_time) AS stat_date,
        eosd.entryid,
        eosd.customid,
        COUNT(DISTINCT eosd.order_id) AS b2b_order_count,
        SUM(eosd.sales_amount) AS b2b_sales_amount
    FROM dwd.eshop_order_sales_doc eosd
    CROSS JOIN date_range dr
    WHERE eosd.process_status IN ('待发货', '发货中', '已发货', '已完成')
    AND DATE(eosd.create_date) >= dr.start_date AND DATE(eosd.create_date) < dr.end_date
    GROUP BY DATE(eosd.create_time), eosd.entryid, eosd.customid
),
-- 计算b2b自主下单订单信息
b2b_self_initiated AS (
    SELECT 
        DATE(eosd.create_time) AS stat_date,
        eosd.entryid,
        eosd.customid,
        COUNT(DISTINCT eosd.order_id) AS b2b_self_initiated_order_count,
        SUM(eosd.sales_amount) AS b2b_self_initiated_sales_amount
    FROM dwd.eshop_order_sales_doc eosd
    CROSS JOIN date_range dr
    WHERE eosd.process_status IN ('待发货', '发货中', '已发货', '已完成')
        AND eosd.is_salesman_order = 0
        AND DATE(eosd.create_date) >= dr.start_date AND DATE(eosd.create_date) < dr.end_date
    GROUP BY DATE(eosd.create_time), eosd.entryid, eosd.customid
),
-- 合并维度信息
dim_info AS (
    SELECT DISTINCT
        e.entryid,
        e.entry_name,
        c.customid,
        c.customer_name
    FROM dim.entry e
    CROSS JOIN dim.customer c
)
-- 最终合并所有数据
SELECT
    COALESCE(ss.stat_date, pb.stat_date, bo.stat_date, bs.stat_date) AS stat_date,
    COALESCE(ss.entryid, pb.entryid, bo.entryid, bs.entryid, di.entryid) AS entryid,
    COALESCE(ss.customid, pb.customid, bo.customid, bs.customid, di.customid) AS customid,
    di.entry_name,
    di.customer_name,
    COALESCE(ss.order_count, 0) AS order_count,
    COALESCE(ss.sales_amount, 0) AS sales_amount,
    COALESCE(pb.potential_b2b_order_count, 0) AS potential_b2b_order_count,
    COALESCE(pb.potential_b2b_sales_amount, 0) AS potential_b2b_sales_amount,
    COALESCE(bo.b2b_order_count, 0) AS b2b_order_count,
    COALESCE(bo.b2b_sales_amount, 0) AS b2b_sales_amount,
    COALESCE(bs.b2b_self_initiated_order_count, 0) AS b2b_self_initiated_order_count,
    COALESCE(bs.b2b_self_initiated_sales_amount, 0) AS b2b_self_initiated_sales_amount
FROM (
    -- 先获取所有有数据的日期、独立单元和客户组合
    SELECT 
        stat_date, entryid, customid
    FROM sales_summary
    UNION
    SELECT 
        stat_date, entryid, customid
    FROM potential_b2b
    UNION
    SELECT 
        stat_date, entryid, customid
    FROM b2b_orders
    UNION
    SELECT 
        stat_date, entryid, customid
    FROM b2b_self_initiated
) base
JOIN dim_info di ON base.entryid = di.entryid AND base.customid = di.customid
LEFT JOIN sales_summary ss ON base.stat_date = ss.stat_date AND base.entryid = ss.entryid AND base.customid = ss.customid
LEFT JOIN potential_b2b pb ON base.stat_date = pb.stat_date AND base.entryid = pb.entryid AND base.customid = pb.customid
LEFT JOIN b2b_orders bo ON base.stat_date = bo.stat_date AND base.entryid = bo.entryid AND base.customid = bo.customid
LEFT JOIN b2b_self_initiated bs ON base.stat_date = bs.stat_date AND base.entryid = bs.entryid AND base.customid = bs.customid;