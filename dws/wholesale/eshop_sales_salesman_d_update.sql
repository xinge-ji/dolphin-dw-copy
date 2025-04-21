DELETE FROM dws.eshop_sales_salesman_d
WHERE stat_date > CURRENT_DATE() - INTERVAL 60 DAY;

-- 插入数据
INSERT INTO dws.eshop_sales_salesman_d (
    stat_date,
    entryid,
    salerid,
    entry_name,
    saler_name,
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
-- 计算可转化为b2b的手工订单信息
potential_b2b AS (
    SELECT 
        DATE(wos.create_date) AS stat_date,
        wos.entryid,
        wos.entry_name,
        wos.customid,
        wos.customer_name,
        wos.salerid,
        wos.saler_name,
        COUNT(DISTINCT wos.salesid) AS potential_b2b_order_count,
        SUM(wosd.sales_amount) AS potential_b2b_sales_amount
    FROM dwd.wholesale_order_sales_doc wos
    JOIN dwd.wholesale_order_sales_dtl wosd ON wos.salesid = wosd.salesid
    CROSS JOIN date_range dr
    WHERE (
        (wos.entryid=1 AND wos.salesdeptid = 26) -- 厦门
        OR (wos.entryid=164 AND wos.salesdeptid = 157688) -- 龙岩
        OR (wos.entryid=204 AND wos.salesdeptid = 62263) -- 三明
        OR (wos.entryid=124 AND wos.salesdeptid = 157615) -- 南平
        OR (wos.entryid=224 AND wos.salesdeptid = 35063) -- 宁德
        OR (wos.entryid=2 AND wos.salesdeptid = 158948) -- 福州
        OR (wos.entryid=104 AND wos.salesdeptid = 28806) -- 莆田
        OR (wos.entryid=144 AND wos.salesdeptid = 30676) -- 泉州
        OR (wos.entryid=5 AND wos.salesdeptid = 92038) -- 漳州
    )
        AND wos.comefrom in ('手工录入', '订单生成')
        AND IFNULL(wos.is_haixi, 0) = 0
        AND IFNULL(wos.is_yaoxiewang, 0) = 0
        AND IFNULL(wosd.storage_name, '') in ('', '三明鹭燕合格保管帐','漳州鹭燕大库保管帐','泉州鹭燕大库保管帐','莆田鹭燕大库保管帐','福州鹭燕大库保管帐','宁德鹭燕大库保管帐','龙岩新鹭燕大库保管帐','南平鹭燕大库保管帐','股份厦门大库保管帐')
        AND DATE(wosd.create_date) >= dr.start_date AND DATE(wosd.create_date) < dr.end_date
    GROUP BY DATE(wos.create_date), wos.entryid, wos.entry_name, 
    wos.customid, wos.customer_name, wos.salerid, wos.saler_name
),
-- 计算b2b订单信息
b2b_orders AS (
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.entry_name,
        wosd.customid,
        wosd.customer_name,
        wosd.salerid,
        wosd.saler_name,
        COUNT(DISTINCT wosd.salesid) AS b2b_order_count,
        SUM(wosd.sales_amount) AS b2b_sales_amount
    FROM dwd.wholesale_order_sales_doc wosd
    CROSS JOIN date_range dr
    WHERE wosd.econid is not null
    AND wosd.use_status in ('正式','临时')
    AND DATE(wosd.create_date) >= dr.start_date AND DATE(wosd.create_date) < dr.end_date
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.entry_name, 
    wosd.customid, wosd.customer_name, wosd.salerid, wosd.saler_name
),
-- 计算b2b自主下单订单信息
b2b_self_initiated AS (
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.entry_name,
        wosd.customid,
        wosd.customer_name,
        wosd.salerid,
        wosd.saler_name,
        COUNT(DISTINCT eosd.order_id) AS b2b_self_initiated_order_count,
        SUM(eosd.sales_amount) AS b2b_self_initiated_sales_amount
    FROM dwd.wholesale_order_sales_doc wosd
    JOIN dwd.eshop_order_sales_doc eosd ON wosd.ordernum = eosd.order_num
    CROSS JOIN date_range dr
    WHERE wosd.econid is not null
    AND wosd.use_status in ('正式','临时')
    AND eosd.is_salesman_order = 0
    AND DATE(wosd.create_date) >= dr.start_date AND DATE(wosd.create_date) < dr.end_date
    GROUP BY 
        DATE(wosd.create_date), wosd.entryid, wosd.entry_name, 
        wosd.customid, wosd.customer_name, wosd.salerid, wosd.saler_name
),
-- 获取所有有数据的日期、独立单元和销售员组合
base_data AS (
    SELECT 
        stat_date, entryid, entry_name, customid, customer_name, salerid, saler_name
    FROM potential_b2b
    UNION
    SELECT 
        stat_date, entryid, entry_name, customid, customer_name, salerid, saler_name
    FROM b2b_orders
    UNION
    SELECT 
        stat_date, entryid, entry_name, customid, customer_name, salerid, saler_name
    FROM b2b_self_initiated
),
-- 合并维度信息
dim_info AS (
    SELECT DISTINCT stat_date, entryid, entry_name, customid, customer_name, salerid, saler_name
    FROM base_data
)
-- 最终合并所有数据
SELECT
    bd.stat_date,
    bd.entryid,
    bd.salerid,
    MAX(bd.entry_name) as entry_name,
    MAX(bd.saler_name) as saler_name,
    SUM(COALESCE(pb.potential_b2b_order_count, 0)) AS potential_b2b_order_count,
    SUM(COALESCE(pb.potential_b2b_sales_amount, 0)) AS potential_b2b_sales_amount,
    SUM(COALESCE(bo.b2b_order_count, 0)) AS b2b_order_count,
    SUM(COALESCE(bo.b2b_sales_amount, 0)) AS b2b_sales_amount,
    SUM(COALESCE(bs.b2b_self_initiated_order_count, 0)) AS b2b_self_initiated_order_count,
    SUM(COALESCE(bs.b2b_self_initiated_sales_amount, 0)) AS b2b_self_initiated_sales_amount
FROM base_data bd
LEFT JOIN potential_b2b pb ON bd.stat_date = pb.stat_date AND bd.entryid = pb.entryid 
    AND bd.customid = pb.customid AND bd.salerid = pb.salerid
LEFT JOIN b2b_orders bo ON bd.stat_date = bo.stat_date AND bd.entryid = bo.entryid 
    AND bd.customid = bo.customid AND bd.salerid = bo.salerid
LEFT JOIN b2b_self_initiated bs ON bd.stat_date = bs.stat_date AND bd.entryid = bs.entryid 
    AND bd.customid = bs.customid AND bd.salerid = bs.salerid
GROUP BY
    bd.stat_date, bd.entryid, bd.salerid;