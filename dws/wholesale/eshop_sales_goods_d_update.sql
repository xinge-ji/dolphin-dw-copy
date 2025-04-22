DELETE FROM dws.eshop_sales_goods_d
WHERE stat_date > CURRENT_DATE() - INTERVAL 60 DAY;

-- 插入最近60天的数据
INSERT INTO dws.eshop_sales_goods_d (
    stat_date,
    entryid,
    goodsid,
    entry_name,
    city_name,
    city_order,
    goods_name,
    nianbao_type,
    group_manage_type,
    variety_level1_name,
    variety_level2_name,
    variety_level3_name,
    order_item_count,
    sales_amount,
    potential_b2b_order_item_count,
    potential_b2b_sales_amount,
    b2b_order_item_count,
    b2b_sales_amount,
    b2b_self_initiated_order_item_count,
    b2b_self_initiated_sales_amount
)
WITH date_range AS (
    SELECT 
        CURRENT_DATE() - INTERVAL 60 DAY AS start_date,
        CURRENT_DATE() AS end_date
),
-- 计算销售细单信息
sales_summary AS (
    SELECT
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.entry_name,
        wosd.customid,
        wosd.goodsid,
        COUNT(DISTINCT wosd.salesdtlid) AS order_item_count,
        SUM(wosd.sales_amount) AS sales_amount
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
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.entry_name, wosd.customid, wosd.goodsid
),
-- 计算可转化为b2b的手工订单信息
potential_b2b AS (
    WITH valid_salesids AS (
        SELECT 
            wosd.salesid,
            MIN(CASE WHEN eeg.goodsid IS NOT NULL OR wosd.goodsid = 37697 THEN 1 ELSE 0 END) AS is_valid
        FROM dwd.wholesale_order_sales_dtl wosd
        LEFT JOIN dim.eshop_entry_goods eeg ON wosd.entryid = eeg.entryid 
            AND wosd.goodsid = eeg.goodsid
            AND wosd.create_date >= eeg.dw_starttime AND wosd.create_date < eeg.dw_endtime
        GROUP BY wosd.salesid
        HAVING is_valid = 1  -- 确保所有明细记录都满足条件
    )
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.entry_name,
        wosd.customid,
        wosd.goodsid,
        COUNT(DISTINCT wosd.salesdtlid) AS potential_b2b_order_item_count,
        SUM(wosd.sales_amount) AS potential_b2b_sales_amount
    FROM dwd.wholesale_order_sales_doc wos
    JOIN valid_salesids vs ON wos.salesid = vs.salesid
    JOIN dwd.wholesale_order_sales_dtl wosd ON wos.salesid = wosd.salesid
    JOIN dim.eshop_entry_customer ecb ON wos.customid = ecb.customid
        AND wos.entryid = ecb.entryid
        AND wos.create_date >= ecb.dw_starttime AND wos.create_date < ecb.dw_endtime
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
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.entry_name, wosd.customid, wosd.goodsid
),
-- 计算b2b订单信息
b2b_orders AS (
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.entry_name,
        wosd.customid,
        wosdtl.goodsid,
        COUNT(DISTINCT wosdtl.salesdtlid) AS b2b_order_item_count,
        SUM(wosdtl.sales_amount) AS b2b_sales_amount
    FROM dwd.wholesale_order_sales_doc wosd
    JOIN dwd.wholesale_order_sales_dtl wosdtl ON wosd.salesid = wosdtl.salesid
    CROSS JOIN date_range dr
    WHERE wosd.econid is not null
    AND wosd.use_status in ('正式','临时')
    AND DATE(wosd.create_date) >= dr.start_date AND DATE(wosd.create_date) < dr.end_date
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.entry_name, wosd.customid, wosdtl.goodsid
),
-- 计算b2b自主下单订单信息
b2b_self_initiated AS (
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.entry_name,
        wosd.customid,
        wosdtl.goodsid,
        COUNT(DISTINCT wosdtl.salesdtlid) AS b2b_self_initiated_order_item_count,
        SUM(wosdtl.sales_amount) AS b2b_self_initiated_sales_amount
    FROM dwd.wholesale_order_sales_doc wosd
    JOIN dwd.eshop_order_sales_doc eosd ON wosd.ordernum = eosd.order_num
    JOIN dwd.wholesale_order_sales_dtl wosdtl ON wosd.salesid = wosdtl.salesid
    CROSS JOIN date_range dr
    WHERE wosd.econid is not null
    AND wosd.use_status in ('正式','临时')
    AND eosd.is_salesman_order = 0
    AND DATE(wosd.create_date) >= dr.start_date AND DATE(wosd.create_date) < dr.end_date
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.entry_name, wosd.customid, wosdtl.goodsid
),
-- 获取所有有数据的日期、独立单元和商品组合
base_data AS (
    SELECT 
        stat_date, entryid, entry_name, customid, goodsid
    FROM sales_summary
    UNION
    SELECT 
        stat_date, entryid, entry_name, customid, goodsid
    FROM potential_b2b
    UNION
    SELECT 
        stat_date, entryid, entry_name, customid, goodsid
    FROM b2b_orders
    UNION
    SELECT 
        stat_date, entryid, entry_name, customid, goodsid
    FROM b2b_self_initiated
),
-- 合并维度信息
dim_info AS (
    SELECT DISTINCT
        bd.stat_date,
        bd.entryid,
        bd.customid,
        bd.goodsid,
        bd.entry_name,
        g.goods_name,
        g.nianbao_type,
        g.group_manage_type,
        g.variety_level1_name,
        g.variety_level2_name,
        g.variety_level3_name
    FROM base_data bd 
    JOIN dim.eshop_entry_customer eshop ON eshop.entryid = bd.entryid AND eshop.customid = bd.customid
        AND bd.stat_date BETWEEN eshop.dw_starttime AND eshop.dw_endtime
    JOIN dim.goods g ON bd.goodsid = g.goodsid
)
-- 最终合并所有数据
SELECT
    bd.stat_date,
    bd.entryid,
    bd.goodsid,
    MAX(bd.entry_name) AS entry_name,
    CASE
        WHEN bd.entryid = 1 THEN '厦门'
        WHEN bd.entryid = 2 THEN '福州'
        WHEN bd.entryid = 5 THEN '漳州'
        WHEN bd.entryid = 104 THEN '莆田'
        WHEN bd.entryid = 124 THEN '南平'
        WHEN bd.entryid = 144 THEN '泉州'
        WHEN bd.entryid = 164 THEN '龙岩'
        WHEN bd.entryid = 204 THEN '三明'
        WHEN bd.entryid = 224 THEN '宁德'
    END AS city_name,
    CASE
        WHEN bd.entryid = 1   THEN 1
        WHEN bd.entryid = 2   THEN 5
        WHEN bd.entryid = 5   THEN 2
        WHEN bd.entryid = 104 THEN 4
        WHEN bd.entryid = 124 THEN 7
        WHEN bd.entryid = 144 THEN 3
        WHEN bd.entryid = 164 THEN 8
        WHEN bd.entryid = 204 THEN 9
        WHEN bd.entryid = 224 THEN 6
    END AS city_order,
    MAX(bd.goods_name) AS goods_name,
    MAX(bd.nianbao_type) AS nianbao_type,
    MAX(bd.group_manage_type) AS group_manage_type,
    MAX(bd.variety_level1_name) AS variety_level1_name,
    MAX(bd.variety_level2_name) AS variety_level2_name,
    MAX(bd.variety_level3_name) AS variety_level3_name,
    SUM(COALESCE(ss.order_item_count, 0)) AS order_item_count,
    SUM(COALESCE(ss.sales_amount, 0)) AS sales_amount,
    SUM(COALESCE(pb.potential_b2b_order_item_count, 0)) AS potential_b2b_order_item_count,
    SUM(COALESCE(pb.potential_b2b_sales_amount, 0)) AS potential_b2b_sales_amount,
    SUM(COALESCE(bo.b2b_order_item_count, 0)) AS b2b_order_item_count,
    SUM(COALESCE(bo.b2b_sales_amount, 0)) AS b2b_sales_amount,
    SUM(COALESCE(bs.b2b_self_initiated_order_item_count, 0)) AS b2b_self_initiated_order_item_count,
    SUM(COALESCE(bs.b2b_self_initiated_sales_amount, 0)) AS b2b_self_initiated_sales_amount
FROM dim_info bd
LEFT JOIN sales_summary ss ON bd.stat_date = ss.stat_date AND bd.entryid = ss.entryid 
    AND bd.customid = ss.customid AND bd.goodsid = ss.goodsid
LEFT JOIN potential_b2b pb ON bd.stat_date = pb.stat_date AND bd.entryid = pb.entryid 
    AND bd.customid = pb.customid AND bd.goodsid = pb.goodsid
LEFT JOIN b2b_orders bo ON bd.stat_date = bo.stat_date AND bd.entryid = bo.entryid 
    AND bd.customid = bo.customid AND bd.goodsid = bo.goodsid
LEFT JOIN b2b_self_initiated bs ON bd.stat_date = bs.stat_date AND bd.entryid = bs.entryid 
    AND bd.customid = bs.customid AND bd.goodsid = bs.goodsid
WHERE bd.stat_date>=date('1970-01-01')
AND bd.entryid in (1,2,5,104,124,144,164,204,224)
GROUP BY 
    bd.stat_date, bd.entryid, bd.goodsid;