DELETE FROM dws.eshop_sales_salesman_d
WHERE stat_date > CURRENT_DATE() - INTERVAL 60 DAY;

-- 插入数据
INSERT INTO dws.eshop_sales_salesman_d (
    stat_date,
    entryid,
    salesman_id,
    entry_name,
    salesman_name,
    potential_b2b_order_count,
    potential_b2b_sales_amount,
    b2b_order_count,
    b2b_sales_amount,
    b2b_self_initiated_order_count,
    b2b_self_initiated_sales_amount
)
WITH 
-- 计算可转化为b2b的手工订单信息
potential_b2b AS (
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        ecs.salesman_id,
        COUNT(DISTINCT wosd.salesid) AS potential_b2b_order_count,
        SUM(wosd.sales_amount) AS potential_b2b_sales_amount
    FROM dwd.wholesale_order_sales_dtl wosd
    JOIN dim.eshop_entry_goods eeg ON wosd.entryid = eeg.entryid 
        AND wosd.goodsid = eeg.goodsid
        AND wosd.create_date BETWEEN eeg.eshop_starttime AND eeg.eshop_endtime
    JOIN dim.eshop_customer_salesman ecs ON wosd.customid = ecs.customid
        AND wosd.entryid = ecs.entryid
        AND wosd.create_date BETWEEN ecs.eshop_starttime AND ecs.eshop_endtime
    WHERE wosd.comefrom = '手工录入'
        AND (wosd.storageid IS NULL 
             OR wosd.storage_name in ('三明鹭燕合格保管帐','漳州鹭燕大库保管帐','泉州鹭燕大库保管帐','莆田鹭燕大库保管帐','福州鹭燕大库保管帐','宁德鹭燕大库保管帐','龙岩新鹭燕大库保管帐','南平鹭燕大库保管帐','股份厦门大库保管帐')
             )
        AND wosd.use_status = '正式'
        AND wosd.sale_type = '销售'
        AND wosd.is_dianshang = 0
        AND wosd.discount >= 1
        AND wosd.create_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    GROUP BY DATE(wosd.create_date), wosd.entryid, ecs.salesman_id
),
-- 计算b2b订单信息
b2b_orders AS (
    SELECT 
        DATE(eosd.create_time) AS stat_date,
        eosd.entryid,
        eosd.salesman_id,
        COUNT(DISTINCT eosd.order_id) AS b2b_order_count,
        SUM(eosd.sales_amount) AS b2b_sales_amount
    FROM dwd.eshop_order_sales_doc eosd
    WHERE eosd.process_status IN ('待发货', '发货中', '已发货', '已完成')
    AND eosd.create_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    GROUP BY DATE(eosd.create_time), eosd.entryid, eosd.salesman_id
),
-- 计算b2b自主下单订单信息
b2b_self_initiated AS (
    SELECT 
        DATE(eosd.create_time) AS stat_date,
        eosd.entryid,
        eosd.salesman_id,
        COUNT(DISTINCT eosd.order_id) AS b2b_self_initiated_order_count,
        SUM(eosd.sales_amount) AS b2b_self_initiated_sales_amount
    FROM dwd.eshop_order_sales_doc eosd
    WHERE eosd.process_status IN ('待发货', '发货中', '已发货', '已完成')
        AND eosd.is_salesman_order = 0
        AND eosd.create_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    GROUP BY DATE(eosd.create_time), eosd.entryid, eosd.salesman_id
),
-- 获取所有有数据的日期、独立单元和销售员组合
base_data AS (
    SELECT 
        stat_date, entryid, salesman_id
    FROM potential_b2b
    UNION
    SELECT 
        stat_date, entryid, salesman_id
    FROM b2b_orders
    UNION
    SELECT 
        stat_date, entryid, salesman_id
    FROM b2b_self_initiated
),
-- 合并维度信息
dim_info AS (
    SELECT 
        e.entryid,
        e.entry_name,
        e.province_name,
        e.city_name,
        e.area_name,
        e.caiwu_level1,
        e.caiwu_level2,
        s.salesman_id,
        s.salesman_name
    FROM (SELECT DISTINCT entryid FROM base_data) t1
    JOIN dim.entry e ON t1.entryid = e.entryid
    JOIN (SELECT DISTINCT salesman_id FROM base_data) t2
    JOIN (
        SELECT 
            salesman_id, 
            MAX(salesman_name) AS salesman_name 
        FROM dwd.eshop_order_sales_doc 
        GROUP BY salesman_id
    ) s ON t2.salesman_id = s.salesman_id
)
-- 最终合并所有数据
SELECT
    bd.stat_date,
    bd.entryid,
    bd.salesman_id,
    di.entry_name,
    di.province_name,
    di.city_name,
    di.area_name,
    di.caiwu_level1,
    di.caiwu_level2,
    di.salesman_name,
    COALESCE(pb.potential_b2b_order_count, 0) AS potential_b2b_order_count,
    COALESCE(pb.potential_b2b_sales_amount, 0) AS potential_b2b_sales_amount,
    COALESCE(bo.b2b_order_count, 0) AS b2b_order_count,
    COALESCE(bo.b2b_sales_amount, 0) AS b2b_sales_amount,
    COALESCE(bs.b2b_self_initiated_order_count, 0) AS b2b_self_initiated_order_count,
    COALESCE(bs.b2b_self_initiated_sales_amount, 0) AS b2b_self_initiated_sales_amount
FROM base_data bd
JOIN dim_info di ON bd.entryid = di.entryid AND bd.salesman_id = di.salesman_id
LEFT JOIN potential_b2b pb ON bd.stat_date = pb.stat_date AND bd.entryid = pb.entryid AND bd.salesman_id = pb.salesman_id
LEFT JOIN b2b_orders bo ON bd.stat_date = bo.stat_date AND bd.entryid = bo.entryid AND bd.salesman_id = bo.salesman_id
LEFT JOIN b2b_self_initiated bs ON bd.stat_date = bs.stat_date AND bd.entryid = bs.entryid AND bd.salesman_id = bs.salesman_id;