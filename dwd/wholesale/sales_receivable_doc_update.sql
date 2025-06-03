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
WITH sales_doc_summary AS (
    -- 直接从 dwd.wholesale_sales_receivable_dtl 按销售单汇总
    SELECT 
        salesid,
        MIN(create_date) as create_date,
        MIN(yewu_date) as yewu_date,
        MAX(sale_mode) as sale_mode,
        SUM(IFNULL(sales_amount, 0)) as total_sales_amount,
        SUM(IFNULL(settle_amount, 0)) as total_settle_amount,
        MAX(settle_time) as max_settle_time,
        SUM(IFNULL(received_amount, 0)) as total_received_amount,
        MAX(received_time) as max_received_time,
        -- 判断销售单是否完全结算：所有明细都已结算
        CASE WHEN MIN(is_settled) = 1 THEN 1 ELSE 0 END as is_fully_settled,
        -- 判断销售单是否完全还款：所有明细都已还款
        CASE WHEN MIN(is_received) = 1 THEN 1 ELSE 0 END as is_fully_received
    FROM dwd.wholesale_sales_receivable_dtl
    GROUP BY salesid
)
select
    salesid,
    create_date,
    yewu_date,
    sale_mode,
    total_sales_amount as sales_amount,
    total_settle_amount as settle_amount,
    CASE WHEN is_fully_settled = 1 THEN max_settle_time ELSE NULL END as settle_time,
    is_fully_settled as is_settled,
    total_received_amount as received_amount,
    CASE WHEN is_fully_received = 1 THEN max_received_time ELSE NULL END as received_time,
    is_fully_received as is_received
from sales_doc_summary;
