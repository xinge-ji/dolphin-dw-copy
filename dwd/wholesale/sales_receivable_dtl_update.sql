insert into dwd.wholesale_sales_receivable_dtl (
    salesdtlid,
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
WITH unsettled_salesdtlids AS (
    -- 从 dwd.wholesale_sales_receivable_dtl 获取未完全结算的 salesdtlid
    SELECT DISTINCT salesdtlid
    FROM dwd.wholesale_sales_receivable_dtl
    WHERE is_settled = 0 OR is_settled IS NULL
),
unpaid_salesdtlids AS (
    -- 从 dwd.wholesale_sales_receivable_dtl 获取未完全付款的 salesdtlid
    SELECT DISTINCT salesdtlid
    FROM dwd.wholesale_sales_receivable_dtl
    WHERE is_received = 0 OR is_received IS NULL
),
settle_summary AS (
    SELECT 
        a.salesdtlid,
        a.sales_amount,
        a.settle_status,
        a.yewu_date,
        MAX(IFNULL(b.confirm_date, a.yewu_date)) AS max_confirm_date,
        SUM(IFNULL(b.settle_amount, 0)) as total_settle_amount
    FROM dwd.wholesale_order_sales_dtl a
    INNER JOIN unsettled_salesdtlids u ON a.salesdtlid = u.salesdtlid  -- 只处理未完全结算的
    LEFT JOIN dwd.wholesale_order_settle_dtl b ON a.salesdtlid = b.salesdtlid
    GROUP BY a.salesdtlid, a.sales_amount, a.settle_status, a.yewu_date
),
fully_settled_order_dtl AS (
    SELECT 
        salesdtlid,
        max_confirm_date,
        total_settle_amount as settle_amount
    FROM settle_summary
    WHERE settle_status = '不结算' 
       OR sales_amount = 0 
       OR abs(total_settle_amount) >= abs(sales_amount)
),
repay_summary AS (
    -- 先计算每个结算单明细的还款汇总，只处理未完全付款的
    SELECT 
        s.sasettledtlid,
        s.salesid,
        s.salesdtlid,
        s.settle_amount,
        s.received_status,
        s.confirm_date,
        MAX(IFNULL(r.payment_date, s.confirm_date)) AS max_payment_date,
        SUM(IFNULL(r.payment_amount, 0)) as total_received_amount
    FROM dwd.wholesale_order_settle_dtl s
    INNER JOIN unpaid_salesdtlids up ON s.salesdtlid = up.salesdtlid  -- 只处理未完全付款的
    LEFT JOIN dwd.wholesale_order_repay_dtl r ON s.sasettledtlid = r.sasettledtlid
    GROUP BY s.sasettledtlid, s.salesid, s.salesdtlid, s.settle_amount, s.received_status, s.confirm_date
),
fully_paid_settle_dtl AS (
    -- 基于还款汇总判断是否结清
    SELECT 
        sasettledtlid,
        salesid,
        salesdtlid,
        max_payment_date,
        1 as is_fully_paid,
        total_received_amount as received_amount
    FROM repay_summary
    WHERE received_status = '不收款' 
       OR settle_amount = 0 
       OR abs(total_received_amount) >= abs(settle_amount)
),
fully_paid_order_dtl AS (
    -- 基于结算单明细的还款状态，计算销售单明细是否完全还清
    SELECT 
        salesdtlid,
        MIN(IFNULL(is_fully_paid, 0)) AS all_paid,  -- 只有当所有关联的结算单明细都已还清时，销售单明细才算完全还清
        MAX(max_payment_date) AS max_payment_date,
        SUM(received_amount) AS received_amount
    FROM fully_paid_settle_dtl
    WHERE salesdtlid IS NOT NULL
    GROUP BY salesdtlid
    HAVING MIN(IFNULL(is_fully_paid, 0)) = 1  -- 所有关联的结算单明细都已还清
)
select
    a.salesdtlid,
    a.salesid,
    a.create_date,
    a.yewu_date,
    a.sale_mode,
    IFNULL(a.sales_amount, 0),
    IFNULL(b.settle_amount, 0),
    b.max_confirm_date as settle_time,
    CASE WHEN b.max_confirm_date IS NOT NULL THEN 1 ELSE 0 END as is_settled,
    IFNULL(c.received_amount, 0),
    c.max_payment_date as received_time,
    CASE WHEN c.max_payment_date IS NOT NULL THEN 1 ELSE 0 END as is_received
from dwd.wholesale_order_sales_dtl a
left join fully_settled_order_dtl b on a.salesdtlid = b.salesdtlid
left join fully_paid_order_dtl c on a.salesdtlid = c.salesdtlid;