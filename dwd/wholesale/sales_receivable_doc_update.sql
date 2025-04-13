
insert into dwd.wholesale_sales_receivable_doc (
    salesid,
    create_date,
    yewu_date,
    sale_mode,
    sales_amount,
    settle_amount,
    settle_time,
    is_settled,
    received_amount,
    received_time,
    is_received
)
WITH fully_settled_order AS (
    -- 计算销售单是否完全结算
    SELECT 
        a.salesid,
        CASE
            WHEN MIN(IFNULL(b.is_settled, 0)) = 1 THEN MAX(b.settle_time) 
            ELSE NULL
        END AS settle_time,
        SUM(b.settle_amount) AS settle_amount
    FROM dwd.wholesale_order_sales_doc a
    LEFT JOIN dwd.wholesale_sales_receivable_dtl b ON a.salesid = b.salesid
    GROUP BY a.salesid
),
fully_paid_order AS (
    -- 计算销售单是否完全还款
    SELECT 
        a.salesid,
        CASE
            WHEN MIN(IFNULL(b.is_received, 0)) = 1 THEN MAX(b.received_time) 
            ELSE NULL
        END AS received_time,
        SUM(b.received_amount) AS received_amount
    FROM dwd.wholesale_order_sales_doc a
    LEFT JOIN dwd.wholesale_sales_receivable_dtl b ON a.salesid = b.salesid
    GROUP BY a.salesid
)
SELECT
    a.salesid,
    a.create_date,
    d.yewu_date,
    a.sale_mode,
    a.sales_amount,
    IFNULL(b.settle_amount, 0) AS settle_amount,
    b.settle_time,
    CASE WHEN b.settle_time IS NOT NULL THEN 1 ELSE 0 END AS is_settled,
    IFNULL(c.received_amount, 0) AS received_amount,
    c.received_time,
    CASE WHEN c.received_time IS NOT NULL THEN 1 ELSE 0 END AS is_received
FROM dwd.wholesale_order_sales_doc a
LEFT JOIN (SELECT salesid, MIN(yewu_date) AS yewu_date FROM dwd.wholesale_order_sales_dtl GROUP BY salesid) d ON a.salesid = d.salesid
LEFT JOIN fully_settled_order b ON a.salesid = b.salesid
LEFT JOIN fully_paid_order c ON a.salesid = c.salesid;
