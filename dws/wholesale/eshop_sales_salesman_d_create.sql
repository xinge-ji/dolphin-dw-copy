DROP TABLE IF EXISTS dws.eshop_sales_salesman_d;

CREATE TABLE dws.eshop_sales_salesman_d (
    -- 维度信息
    stat_date DATE COMMENT "业务日期",
    entryid bigint COMMENT "独立单元ID",
    salerid bigint COMMENT "销售员ID",

    -- 组织信息
    entry_name varchar COMMENT "独立单元名称",

    -- 销售员信息
    saler_name varchar COMMENT "销售员名称",

    -- 可转化为b2b的手工订单信息
    potential_b2b_order_count int COMMENT "可转化为b2b的订单数量",
    potential_b2b_sales_amount decimal(18,4) COMMENT "可转化为b2b的订单金额",

    -- b2b订单信息
    b2b_order_count int COMMENT "b2b订单数量",
    b2b_sales_amount decimal(18,4) COMMENT "b2b订单金额",

    -- b2b自主下单订单信息
    b2b_self_initiated_order_count int COMMENT "b2b自主下单订单数量",
    b2b_self_initiated_sales_amount decimal(18,4) COMMENT "b2b自主下单订单金额"
)
UNIQUE KEY(stat_date, entryid, salerid) 
DISTRIBUTED BY HASH(stat_date, entryid, salerid) 
PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

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
WITH 
-- 计算可转化为b2b的手工订单信息
potential_b2b AS (
    SELECT 
        DATE(wosd.create_date) AS stat_date,
        wosd.entryid,
        wosd.entry_name,
        wosd.customid,
        wosd.customer_name,
        wosd.salerid,
        wosd.saler_name,
        COUNT(DISTINCT wosd.salesid) AS potential_b2b_order_count,
        SUM(wosdtl.sales_amount) AS potential_b2b_sales_amount
    FROM dwd.wholesale_order_sales_doc wosd
    JOIN dwd.wholesale_order_sales_dtl wosdtl ON wosd.salesid = wosdtl.salesid
    JOIN dim.eshop_entry_customer eec ON wosd.customid = eec.customid
        AND wosd.entryid = eec.entryid
        AND wosd.create_date BETWEEN eec.dw_starttime AND eec.dw_endtime
    JOIN dim.eshop_entry_goods eeg ON wosdtl.goodsid = eeg.goodsid
        AND wosd.entryid = eeg.entryid
        AND wosd.create_date BETWEEN eeg.dw_starttime AND eeg.dw_endtime
    WHERE wosd.comefrom = '手工录入'
        AND (wosdtl.storageid IS NULL 
             OR wosdtl.storage_name in ('三明鹭燕合格保管帐','漳州鹭燕大库保管帐','泉州鹭燕大库保管帐','莆田鹭燕大库保管帐','福州鹭燕大库保管帐','宁德鹭燕大库保管帐','龙岩新鹭燕大库保管帐','南平鹭燕大库保管帐','股份厦门大库保管帐')
             )
        AND wosd.use_status = '正式'
        AND wosd.sale_type = '销售'
        AND wosdtl.is_dianshang = 0
        AND wosdtl.discount >= 1
    GROUP BY DATE(wosd.create_date), wosd.entryid, wosd.entry_name, 
    wosd.customid, wosd.customer_name, wosd.salerid, wosd.saler_name
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
    WHERE wosd.econid is not null
    AND wosd.use_status in ('正式','临时')
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
    WHERE wosd.econid is not null
    AND wosd.use_status in ('正式','临时')
    AND eosd.is_salesman_order = 0
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
    MAX(bd.saler_name) as salesman_name,
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