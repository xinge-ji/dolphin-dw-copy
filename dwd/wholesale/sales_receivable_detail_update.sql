insert into dwd.wholesale_sales_receivable_detail(
    salesid,
    salesdtlid,
    entryid,
    entry_name,
    province_name,
    city_name,
    area_name,
    caiwu_level1,
    caiwu_level2,
    customid,
    customer_name,
    customertype_name,
    customertype_group,
    customer_financeclass_name,
    is_btob,
    jicai_type,
    is_jicai_zhongxuan,
    comefrom,
    create_date,
    yewu_date,
    sale_mode,
    item_sales_settle_status,
    item_sales_amount,
    order_settle_amount,
    order_settle_time,
    order_settle_status,
    order_item_settle_amount,
    order_item_settle_time,
    order_item_settle_status,
    order_payment_amount,
    order_payment_time,
    order_payment_status,
    order_item_payment_amount,
    order_item_payment_time,
    order_item_payment_status
)
WITH fully_settled_order AS (
    SELECT 
        a.salesid,
        MIN(CASE WHEN a.settle_status = '结算完成' OR b.settle_status in ('已收完', '不收款') THEN 1 ELSE 0 END) AS all_settled,
        MAX(b.confirm_date) AS max_confirm_date
    FROM dwd.wholesale_order_sales_dtl a
    LEFT JOIN dwd.wholesale_order_settle_dtl b ON a.salesdtlid = b.salesdtlid
    WHERE a.salesid IS NOT NULL and a.create_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    GROUP BY a.salesid
    HAVING MIN(CASE WHEN a.settle_status = '结算完成' OR b.settle_status in ('已收完', '不收款') THEN 1 ELSE 0 END) = 1
),
fully_settled_order_item AS (
    SELECT 
        a.salesdtlid,
        MIN(CASE WHEN a.settle_status = '结算完成' OR b.settle_status in ('已收完', '不收款') THEN 1 ELSE 0 END) AS all_settled,
        MAX(b.confirm_date) AS max_confirm_date
    FROM dwd.wholesale_order_sales_dtl a
    LEFT JOIN dwd.wholesale_order_settle_dtl b ON a.salesdtlid = b.salesdtlid
    WHERE a.salesdtlid IS NOT NULL and a.create_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    GROUP BY a.salesdtlid
    HAVING MIN(CASE WHEN a.settle_status = '结算完成' OR b.settle_status in ('已收完', '不收款') THEN 1 ELSE 0 END) = 1
),
fully_paid_settle_dtl AS (
    -- 先计算每个结算单明细的还款总额是否足够结清
    SELECT 
        s.sasettledtlid,
        s.salesid,
        s.salesdtlid,
        MAX(r.payment_date) AS max_payment_date,
        CASE 
            WHEN SUM(IFNULL(r.payment_amount, 0)) >= s.settle_amount THEN 1
            ELSE 0
        END AS is_fully_paid
    FROM dwd.wholesale_order_settle_dtl s
    JOIN fully_settled_order_item fo ON s.salesdtlid = fo.salesdtlid
    LEFT JOIN dwd.wholesale_order_repay_dtl r ON s.sasettledtlid = r.sasettledtlid AND r.use_status = '正式'
    WHERE s.use_status = '正式' and (s.settle_status != '不收款' OR s.settle_status IS NULL)
    GROUP BY s.sasettledtlid, s.salesid, s.salesdtlid, s.settle_amount
),
fully_paid_order AS (
    -- 基于结算单明细的还款状态，计算整个销售单是否完全还清
    SELECT 
        salesid,
        MIN(is_fully_paid) AS all_paid,  -- 只有当所有结算单明细都已还清时，销售单才算完全还清
        MAX(max_payment_date) AS max_payment_date
    FROM fully_paid_settle_dtl
    WHERE salesid IS NOT NULL
    GROUP BY salesid
    HAVING MIN(is_fully_paid) = 1  -- 所有结算单明细都已还清
),
fully_paid_order_item AS (
    -- 基于结算单明细的还款状态，计算销售单明细是否完全还清
    SELECT 
        salesdtlid,
        MIN(is_fully_paid) AS all_paid,  -- 只有当所有关联的结算单明细都已还清时，销售单明细才算完全还清
        MAX(max_payment_date) AS max_payment_date
    FROM fully_paid_settle_dtl
    WHERE salesdtlid IS NOT NULL
    GROUP BY salesdtlid
    HAVING MIN(is_fully_paid) = 1  -- 所有关联的结算单明细都已还清
),
-- 新增CTE来替代子查询
order_settle_amounts AS (
    SELECT 
        salesid,
        SUM(IFNULL(settle_amount, 0)) AS total_settle_amount
    FROM dwd.wholesale_order_settle_dtl
    WHERE use_status = '正式'
    GROUP BY salesid
),
order_item_settle_amounts AS (
    SELECT 
        salesdtlid,
        SUM(IFNULL(settle_amount, 0)) AS total_settle_amount
    FROM dwd.wholesale_order_settle_dtl
    WHERE use_status = '正式'
    GROUP BY salesdtlid
),
order_payment_amounts AS (
    SELECT 
        s.salesid,
        SUM(IFNULL(r.payment_amount, 0)) AS total_payment_amount
    FROM dwd.wholesale_order_settle_dtl s
    LEFT JOIN dwd.wholesale_order_repay_dtl r ON s.sasettledtlid = r.sasettledtlid AND r.use_status = '正式'
    WHERE s.use_status = '正式'
    GROUP BY s.salesid
),
order_item_payment_amounts AS (
    SELECT 
        s.salesdtlid,
        SUM(IFNULL(r.payment_amount, 0)) AS total_payment_amount
    FROM dwd.wholesale_order_settle_dtl s
    LEFT JOIN dwd.wholesale_order_repay_dtl r ON s.sasettledtlid = r.sasettledtlid AND r.use_status = '正式'
    WHERE s.use_status = '正式'
    GROUP BY s.salesdtlid
)
SELECT 
    a.salesid, 
    a.salesdtlid, 
    COALESCE(a.entryid, b.entryid) AS entryid, 
    COALESCE(a.entry_name, b.entry_name) AS entry_name,
    COALESCE(a.province_name, b.province_name) AS province_name,
    COALESCE(a.city_name, b.city_name) AS city_name,
    COALESCE(e.area_name, 'UNKNOWN') AS area_name,
    COALESCE(e.caiwu_level1, 'UNKNOWN') AS caiwu_level1,
    COALESCE(e.caiwu_level2, 'UNKNOWN') AS caiwu_level2,
    COALESCE(a.customid, b.customid) AS customid, 
    COALESCE(a.customer_name, b.customer_name) AS customer_name,
    COALESCE(c.customertype_name, 'UNKNOWN') AS customertype_name,
    COALESCE(c.customertype_group, 'UNKNOWN') AS customertype_group,
    COALESCE(c.customer_financeclass_name, 'UNKNOWN') AS customer_financeclass_name,
    COALESCE(a.is_btob, b.is_btob, 0) AS is_btob,
    COALESCE(a.jicai_type, b.jicai_type) AS jicai_type,
    COALESCE(a.is_jicai_zhongxuan, b.is_jicai_zhongxuan, 0) AS is_jicai_zhongxuan,
    COALESCE(a.comefrom, b.comefrom) AS comefrom,
    a.create_date, 
    a.yewu_date,
    a.sale_mode, 
    a.settle_status AS item_sales_settle_status, 
    a.sales_amount AS item_sales_amount,
    -- 订单级别结算数据
    COALESCE(osa.total_settle_amount, 0) AS order_settle_amount,
    CASE 
        WHEN fs.salesid IS NOT NULL THEN fs.max_confirm_date
        ELSE NULL
    END AS order_settle_time,
    CASE 
        WHEN fs.salesid IS NOT NULL THEN 1
        ELSE 0
    END AS order_settle_status,
    -- 订单明细级别结算数据
    COALESCE(oisa.total_settle_amount, 0) AS order_item_settle_amount,
    CASE
        WHEN fsi.salesdtlid IS NOT NULL THEN fsi.max_confirm_date
        ELSE NULL
    END AS order_item_settle_time,
    CASE
        WHEN fsi.salesdtlid IS NOT NULL THEN 1
        ELSE 0
    END AS order_item_settle_status,
    -- 订单级别还款数据
    COALESCE(opa.total_payment_amount, 0) AS order_payment_amount,
    CASE 
        WHEN fpo.salesid IS NOT NULL THEN fpo.max_payment_date
        ELSE NULL
    END AS order_payment_time,
    CASE 
        WHEN fpo.salesid IS NOT NULL THEN 1
        ELSE 0
    END AS order_payment_status,
    -- 订单明细级别还款数据
    COALESCE(oipa.total_payment_amount, 0) AS order_item_payment_amount,
    CASE
        WHEN fpoi.salesdtlid IS NOT NULL THEN fpoi.max_payment_date
        ELSE NULL
    END AS order_item_payment_time,
    CASE
        WHEN fpoi.salesdtlid IS NOT NULL THEN 1
        ELSE 0
    END AS order_item_payment_status
FROM dwd.wholesale_order_sales_dtl a
LEFT JOIN dwd.wholesale_order_settle_dtl b ON a.salesdtlid = b.salesdtlid
LEFT JOIN fully_settled_order fs ON a.salesid = fs.salesid
LEFT JOIN fully_settled_order_item fsi ON a.salesdtlid = fsi.salesdtlid
LEFT JOIN fully_paid_order fpo ON a.salesid = fpo.salesid
LEFT JOIN fully_paid_order_item fpoi ON a.salesdtlid = fpoi.salesdtlid
LEFT JOIN (SELECT DISTINCT entryid, area_name, caiwu_level1, caiwu_level2 from dim.entry) e ON a.entryid = e.entryid
LEFT JOIN (SELECT DISTINCT customid, customer_name, customertype_name, customertype_group, customer_financeclass_name from dim.customer) c ON a.customid = c.customid
-- 新增JOIN替代子查询
LEFT JOIN order_settle_amounts osa ON a.salesid = osa.salesid
LEFT JOIN order_item_settle_amounts oisa ON a.salesdtlid = oisa.salesdtlid
LEFT JOIN order_payment_amounts opa ON a.salesid = opa.salesid
LEFT JOIN order_item_payment_amounts oipa ON a.salesdtlid = oipa.salesdtlid
WHERE (a.sale_mode ='普通销售' AND (b.settle_status IN ('已收完','未收完') OR b.settle_status IS NULL)
   OR (a.sale_mode IS NULL AND (b.settle_status IN ('已收完','未收完') OR b.settle_status IS NULL)))
AND a.use_status = '正式'
AND (b.use_status = '正式' OR b.use_status IS NULL);