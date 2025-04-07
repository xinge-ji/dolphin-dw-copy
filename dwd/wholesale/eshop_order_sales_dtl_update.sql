INSERT INTO dwd.eshop_order_sales_dtl (order_item_id, __DORIS_DELETE_SIGN__)
SELECT a.order_item_id, 1
FROM ods_dsys.prd_order_item AS a
JOIN dwd.eshop_order_sales_dtl AS b
ON a.order_item_id = b.order_item_id
WHERE a.is_active = 0 AND a.update_time >= b.update_time;

INSERT INTO dwd.eshop_order_sales_dtl (
    order_item_id,
    order_id,
    create_time,
    update_time,
    product_id,
    entryid,
    customid,
    goodsid,
    goods_qty,
    sales_amount
)
SELECT 
    t1.order_item_id,
    t1.order_id,
    t1.create_time,
    t1.update_time,
    t1.product_id,
    t2.entryid,
    t2.customid,
    t3.goodsid,
    t1.num as goods_qty,
    CAST(SUBTOTAL / 10000 as decimal(18,4)) as sales_amount
FROM ods_dsys.prd_order_item t1
JOIN dwd.eshop_order_sales_doc t2 ON t1.order_id = t2.order_id
LEFT JOIN (SELECT entryid, product_id, MIN(goodsid) FROM dim.eshop_entry_goods GROUP BY entryid, product_id) t3 ON t1.product_id = t3.product_id AND t2.entryid = t3.entryid
WHERE t1.update_time>=(SELECT MAX(update_time) - INTERVAL 60 DAY FROM dwd.ec_order_sales_dtl);
