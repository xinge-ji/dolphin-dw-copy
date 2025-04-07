INSERT INTO dwd.eshop_order_sales_doc (orderid, __DORIS_DELETE_SIGN__)
SELECT a.orderid, 1
FROM ods_dsys.prd_order AS a
JOIN dwd.eshop_order_sales_doc AS b
ON a.orderid = b.orderid
WHERE a.is_active = 0 AND a.update_time >= b.update_time;


INSERT INTO dwd.eshop_order_sales_doc (
    order_id,
    create_time,
    update_time,
    org_id,
    entryid,
    buyers_id,
    customid,
    process_stat_code,
    process_status,
    is_salesman_order,
    salesman_id,
    salesman_name,
    sales_amount
) 
SELECT
    t1.order_id,
    t1.create_time,
    t1.update_time,
    t1.org_id,
    t2.entryid,
    t1.buyers_id,
    t3.customid,
    t1.process_stat_code,
    CASE
        WHEN t1.process_stat_code = 'WAIT_APPROVE' THEN '待审核'
        WHEN t1.process_stat_code = 'WAIT_SEND' THEN '待发货'
        WHEN t1.process_stat_code = 'SENDING' THEN '发货中'
        WHEN t1.process_stat_code = 'SEND' THEN '已发货'
        WHEN t1.process_stat_code = 'FINISH' THEN '已完成'
        WHEN t1.process_stat_code = 'CANCEL' THEN '已取消'
        WHEN t1.process_stat_code = 'PARTIAL_FINISH' THEN '部分中止'
        WHEN t1.process_stat_code = 'OA_PARTIAL_FINISH' THEN '完全终止'
        WHEN t1.process_stat_code = 'WAIT_PAY' THEN '待付款'
        WHEN t1.process_stat_code = 'REFUNDING' THEN '退款中'
        WHEN t1.process_stat_code = 'REFUND_FINISH' THEN '退款完成'
        WHEN t1.process_stat_code = 'REFUND_FAILURE' THEN '退款驳回'
        WHEN t1.process_stat_code = 'AUTO_REFUND_FAILURE' THEN '自动退款失败'
        ELSE 'UNKNOWN'
    END as process_status,
    CASE WHEN is_salesman_order = 'Y' THEN 1
         ELSE 0
    END as is_salesman_order,
    t1.salesman_id,
    t4.salesman_name,
    CAST(ORDER_TOTAL_AMOUNT / 10000 as decimal(18,4)) as sales_amount
FROM ods_dsys.prd_order t1
LEFT JOIN dim.entry t2 ON t1.org_id = t2.org_id
LEFT JOIN dim.eshop_customer_buyer t3 ON t1.buyers_id = t3.buyers_id
LEFT JOIN (SELECT salesman_id, MIN(salesman_name) as salesman_name FROM ods_dsys.sys_org_salesman GROUP BY salesman_id) t4
    ON t1.salesman_id = t4.salesman_id
WHERE t1.update_time >= (SELECT MAX(update_time) - INTERVAL 60 DAY FROM dwd.eshop_order_sales_doc);

