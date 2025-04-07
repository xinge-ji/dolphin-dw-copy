DROP TABLE IF EXISTS dws.eshop_sales_goods_d;

CREATE TABLE dws.eshop_sales_goods_d (
    -- 维度信息
    stat_date DATE COMMENT "业务日期",
    entryid bigint COMMENT "独立单元ID",
    goodsid bigint COMMENT "商品ID",

    -- 组织信息
    entry_name varchar COMMENT "独立单元名称",

    -- 商品信息
    goods_name varchar,
    nianbao_type varchar COMMENT "商品年报类型",
    group_manage_type varchar COMMENT "商品集团管理类型",
    variety_level1_name varchar COMMENT "商品分类小类",
    variety_level2_name varchar COMMENT "商品分类中类",
    variety_level3_name varchar COMMENT "商品分类大类",

    -- 销售总单信息
    order_item_count int COMMENT "销售细单数量",
    sales_amount decimal(18,4) COMMENT "销售总金额",

    -- 可转化为b2b的手工订单信息
    potential_b2b_order_item_count int COMMENT "可转化为b2b的订单数量",
    potential_b2b_sales_amount decimal(18,4) COMMENT "可转化为b2b的订单金额",

    -- b2b订单信息
    b2b_order_item_count int COMMENT "b2b订单数量",
    b2b_sales_amount decimal(18,4) COMMENT "b2b订单金额",

    -- b2b自主下单订单信息
    b2b_self_initiated_order_item_count int COMMENT "b2b自主下单订单数量",
    b2b_self_initiated_sales_amount decimal(18,4) COMMENT "b2b自主下单订单金额"
)
UNIQUE KEY(stat_date, entryid, goodsid) 
DISTRIBUTED BY HASH(stat_date, entryid, goodsid) 
PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

-- 插入数据
INSERT INTO dws.eshop_sales_goods_d (
    stat_date,
    entryid,
    goodsid,
    entry_name,
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
WITH 
-- 计算销售总单信息
sales_summary AS (
    SELECT
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.goodsid,
        COUNT(DISTINCT wosd.salesdtlid) AS order_item_count,
        SUM(wosd.sales_amount) AS sales_amount
    FROM dwd.wholesale_order_sales_dtl wosd
    WHERE wosd.use_status = '正式'
    AND wosd.sale_type = '销售'
    AND IFNULL(wosd.is_haixi,0) = 0
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.goodsid
),
-- 计算可转化为b2b的手工订单信息
potential_b2b AS (
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.goodsid,
        COUNT(DISTINCT wosd.salesdtlid) AS potential_b2b_order_item_count,
        SUM(wosd.sales_amount) AS potential_b2b_sales_amount
    FROM dwd.wholesale_order_sales_dtl wosd
    JOIN dim.eshop_entry_goods eeg ON wosd.entryid = eeg.entryid 
        AND wosd.goodsid = eeg.goodsid
        AND wosd.create_date BETWEEN eeg.dw_starttime AND eeg.dw_endtime
    JOIN dim.eshop_entry_customer ecb ON wosd.customid = ecb.customid
        AND wosd.entryid = ecb.entryid
        AND wosd.create_date BETWEEN ecb.dw_starttime AND ecb.dw_endtime
    WHERE wosd.comefrom = '手工录入'
        AND (wosd.storageid IS NULL 
             OR wosd.storage_name in ('三明鹭燕合格保管帐','漳州鹭燕大库保管帐','泉州鹭燕大库保管帐','莆田鹭燕大库保管帐','福州鹭燕大库保管帐','宁德鹭燕大库保管帐','龙岩新鹭燕大库保管帐','南平鹭燕大库保管帐','股份厦门大库保管帐')
             )
        AND wosd.use_status = '正式'
        AND wosd.sale_type = '销售'
        AND wosd.is_dianshang = 0
        AND wosd.discount >= 1
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.goodsid
),
-- 计算b2b订单信息
b2b_orders AS (
    SELECT 
        DATE(eosd.create_time) AS stat_date,
        eosd.entryid,
        eod.goodsid,
        COUNT(DISTINCT eod.order_item_id) AS b2b_order_item_count,
        SUM(eod.sales_amount) AS b2b_sales_amount
    FROM dwd.eshop_order_sales_doc eosd
    JOIN dwd.eshop_order_sales_dtl eod ON eosd.order_id = eod.order_id
    WHERE eosd.process_status IN ('待发货', '发货中', '已发货', '已完成')
    GROUP BY DATE(eosd.create_time), eosd.entryid, eod.goodsid
),
-- 计算b2b自主下单订单信息
b2b_self_initiated AS (
    SELECT 
        DATE(eosd.create_time) AS stat_date,
        eosd.entryid,
        eod.goodsid,
        COUNT(DISTINCT eod.order_item_id) AS b2b_self_initiated_order_item_count,
        SUM(eod.sales_amount) AS b2b_self_initiated_sales_amount
    FROM dwd.eshop_order_sales_doc eosd
    JOIN dwd.eshop_order_sales_dtl eod ON eosd.order_id = eod.order_id
    WHERE eosd.process_status IN ('待发货', '发货中', '已发货', '已完成')
        AND eosd.is_salesman_order = 0
    GROUP BY DATE(eosd.create_time), eosd.entryid, eod.goodsid
),
-- 获取所有有数据的日期、独立单元和商品组合
base_data AS (
    SELECT 
        stat_date, entryid, goodsid
    FROM sales_summary
    UNION
    SELECT 
        stat_date, entryid, goodsid
    FROM potential_b2b
    UNION
    SELECT 
        stat_date, entryid, goodsid
    FROM b2b_orders
    UNION
    SELECT 
        stat_date, entryid, goodsid
    FROM b2b_self_initiated
),
-- 合并维度信息
dim_info AS (
    SELECT DISTINCT
        e.entryid,
        e.entry_name,
        g.goodsid,
        g.goods_name,
        g.nianbao_type,
        g.group_manage_type,
        g.variety_level1_name,
        g.variety_level2_name,
        g.variety_level3_name
    FROM (SELECT DISTINCT entryid FROM base_data) t1
    JOIN dim.entry e ON t1.entryid = e.entryid
    JOIN (SELECT DISTINCT goodsid FROM base_data) t2
    JOIN dim.goods g ON t2.goodsid = g.goodsid
)
-- 最终合并所有数据
SELECT
    bd.stat_date,
    bd.entryid,
    bd.goodsid,
    di.entry_name,
    di.goods_name,
    di.nianbao_type,
    di.group_manage_type,
    di.variety_level1_name,
    di.variety_level2_name,
    di.variety_level3_name,
    COALESCE(ss.order_item_count, 0) AS order_item_count,
    COALESCE(ss.sales_amount, 0) AS sales_amount,
    COALESCE(pb.potential_b2b_order_item_count, 0) AS potential_b2b_order_item_count,
    COALESCE(pb.potential_b2b_sales_amount, 0) AS potential_b2b_sales_amount,
    COALESCE(bo.b2b_order_item_count, 0) AS b2b_order_item_count,
    COALESCE(bo.b2b_sales_amount, 0) AS b2b_sales_amount,
    COALESCE(bs.b2b_self_initiated_order_item_count, 0) AS b2b_self_initiated_order_item_count,
    COALESCE(bs.b2b_self_initiated_sales_amount, 0) AS b2b_self_initiated_sales_amount
FROM base_data bd
JOIN dim_info di ON bd.entryid = di.entryid AND bd.goodsid = di.goodsid
LEFT JOIN sales_summary ss ON bd.stat_date = ss.stat_date AND bd.entryid = ss.entryid AND bd.goodsid = ss.goodsid
LEFT JOIN potential_b2b pb ON bd.stat_date = pb.stat_date AND bd.entryid = pb.entryid AND bd.goodsid = pb.goodsid
LEFT JOIN b2b_orders bo ON bd.stat_date = bo.stat_date AND bd.entryid = bo.entryid AND bd.goodsid = bo.goodsid
LEFT JOIN b2b_self_initiated bs ON bd.stat_date = bs.stat_date AND bd.entryid = bs.entryid AND bd.goodsid = bs.goodsid;