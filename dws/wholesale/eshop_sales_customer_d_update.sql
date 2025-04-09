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
        CURRENT_DATE() AS end_date
),
-- 计算销售总单信息
sales_summary AS (
    SELECT
        DATE(wos.create_date) AS stat_date,
        wos.entryid,
        wos.entry_name,
        wos.customid,
        wos.customer_name,
        COUNT(DISTINCT wos.salesid) AS order_count,
        SUM(wos.sales_amount) AS sales_amount
    FROM dwd.wholesale_order_sales_doc wos
    INNER JOIN (SELECT entryid, MIN(dw_starttime) as dw_starttime FROM dim.eshop_entry_customer group by entryid) ecb 
    ON  wos.entryid = ecb.entryid AND wos.create_date >= ecb.dw_starttime
    CROSS JOIN date_range dr
    WHERE wos.use_status in ('正式','临时')
    AND sale_type = '销售'
    AND IFNULL(is_haixi,0) = 0
    AND DATE(wos.create_date) >= dr.start_date AND DATE(wos.create_date) < dr.end_date
    GROUP BY DATE(wos.create_date), wos.entryid, wos.entry_name, wos.customid, wos.customer_name
),
-- 计算可转化为b2b的手工订单信息
potential_b2b AS (
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.entry_name,
        wosd.customid,
        wosd.customer_name,
        COUNT(DISTINCT wosd.salesid) AS potential_b2b_order_count,
        SUM(wosd.sales_amount) AS potential_b2b_sales_amount
    FROM dwd.wholesale_order_sales_dtl wosd
    JOIN dim.eshop_entry_goods eeg ON wosd.entryid = eeg.entryid 
        AND wosd.goodsid = eeg.goodsid
        AND wosd.create_date >= eeg.dw_starttime AND wosd.create_date < eeg.dw_endtime
    JOIN dim.eshop_entry_customer ecb ON wosd.customid = ecb.customid
        AND wosd.entryid = ecb.entryid
        AND wosd.create_date >= ecb.dw_starttime AND wosd.create_date < ecb.dw_endtime
    CROSS JOIN date_range dr
    WHERE wosd.comefrom = '手工录入'
        AND (wosd.storageid IS NULL 
             OR wosd.storage_name in ('三明鹭燕合格保管帐','漳州鹭燕大库保管帐','泉州鹭燕大库保管帐','莆田鹭燕大库保管帐','福州鹭燕大库保管帐','宁德鹭燕大库保管帐','龙岩新鹭燕大库保管帐','南平鹭燕大库保管帐','股份厦门大库保管帐')
             )
        AND wosd.use_status in ('正式','临时')
        AND wosd.sale_type = '销售'
        AND wosd.is_dianshang = 0
        AND wosd.discount >= 1
        AND DATE(wosd.create_date) >= dr.start_date AND DATE(wosd.create_date) < dr.end_date
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.entry_name, wosd.customid, wosd.customer_name
),
-- 计算b2b订单信息
b2b_orders AS (
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.entry_name,
        wosd.customid,
        wosd.customer_name,
        COUNT(DISTINCT wosd.salesid) AS b2b_order_count,
        SUM(wosd.sales_amount) AS b2b_sales_amount
    FROM dwd.wholesale_order_sales_doc wosd
    CROSS JOIN date_range dr
    WHERE wosd.econid is not null
    AND wosd.use_status in ('正式','临时')
    AND DATE(wosd.create_date) >= dr.start_date AND DATE(wosd.create_date) < dr.end_date
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.entry_name, wosd.customid, wosd.customer_name
),
-- 计算b2b自主下单订单信息
b2b_self_initiated AS (
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.entry_name,
        wosd.customid,
        wosd.customer_name,
        COUNT(DISTINCT wosd.salesid) AS b2b_self_initiated_order_count,
        SUM(eosd.sales_amount) AS b2b_self_initiated_sales_amount
    FROM dwd.wholesale_order_sales_doc wosd
    JOIN dwd.eshop_order_sales_doc eosd ON wosd.ordernum = eosd.order_num
    CROSS JOIN date_range dr
    WHERE wosd.econid is not null
    AND wosd.use_status in ('正式','临时')
    AND eosd.is_salesman_order = 0
    AND DATE(wosd.create_date) >= dr.start_date AND DATE(wosd.create_date) < dr.end_date
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.entry_name, wosd.customid, wosd.customer_name
),
-- 获取所有有数据的日期、独立单元和客户组合
base_data AS (
    SELECT 
        stat_date, entryid, entry_name, customid, customer_name
    FROM sales_summary
    UNION
    SELECT 
        stat_date, entryid, entry_name, customid, customer_name
    FROM potential_b2b
    UNION
    SELECT 
        stat_date, entryid, entry_name, customid, customer_name
    FROM b2b_orders
    UNION
    SELECT 
        stat_date, entryid, entry_name, customid, customer_name
    FROM b2b_self_initiated
),
-- 合并维度信息
dim_info AS (
    SELECT DISTINCT
        bd.stat_date,
        bd.entryid,
        bd.customid,
        bd.entry_name,
        bd.customer_name
    FROM base_data bd
)
-- 最终合并所有数据
SELECT
    bd.stat_date,
    bd.entryid,
    bd.customid,
    MAX(bd.entry_name) as entry_name,
    MAX(bd.customer_name) as customer_name,
    SUM(COALESCE(ss.order_count, 0)) AS order_count,
    SUM(COALESCE(ss.sales_amount, 0)) AS sales_amount,
    SUM(COALESCE(pb.potential_b2b_order_count, 0)) AS potential_b2b_order_count,
    SUM(COALESCE(pb.potential_b2b_sales_amount, 0)) AS potential_b2b_sales_amount,
    SUM(COALESCE(bo.b2b_order_count, 0)) AS b2b_order_count,
    SUM(COALESCE(bo.b2b_sales_amount, 0)) AS b2b_sales_amount,
    SUM(COALESCE(bs.b2b_self_initiated_order_count, 0)) AS b2b_self_initiated_order_count,
    SUM(COALESCE(bs.b2b_self_initiated_sales_amount, 0)) AS b2b_self_initiated_sales_amount
FROM dim_info bd
LEFT JOIN sales_summary ss ON bd.stat_date = ss.stat_date AND bd.entryid = ss.entryid AND bd.customid = ss.customid
LEFT JOIN potential_b2b pb ON bd.stat_date = pb.stat_date AND bd.entryid = pb.entryid AND bd.customid = pb.customid
LEFT JOIN b2b_orders bo ON bd.stat_date = bo.stat_date AND bd.entryid = bo.entryid AND bd.customid = bo.customid
LEFT JOIN b2b_self_initiated bs ON bd.stat_date = bs.stat_date AND bd.entryid = bs.entryid AND bd.customid = bs.customid
GROUP BY bd.stat_date, bd.entryid, bd.customid;