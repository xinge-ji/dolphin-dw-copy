DROP TABLE IF EXISTS dwd.eshop_order_sales_dtl;
CREATE TABLE dwd.eshop_order_sales_dtl (
    order_item_id bigint COMMENT '订单项ID',
	order_id bigint COMMENT '订单ID',
    create_time datetime COMMENT '创建时间',
    update_time datetime COMMENT '更新时间',
    product_id bigint COMMENT '产品ID',
    entryid bigint COMMENT '独立单元ID',
    customid bigint COMMENT '客户ID',
    goodsid bigint COMMENT '商品ID',
    goods_qty decimal(18,4) COMMENT '商品数量',
    sales_amount decimal(18,4) COMMENT '销售金额'
)
UNIQUE KEY(order_item_id) DISTRIBUTED BY HASH(order_item_id) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

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
LEFT JOIN (SELECT entryid, product_id, MIN(goodsid) FROM dim.eshop_entry_goods GROUP BY entryid, product_id) t3 ON t1.product_id = t3.product_id AND t2.entryid = t3.entryid;

CREATE INDEX IF NOT EXISTS idx_order_id ON dwd.eshop_order_sales_dtl (order_id);
CREATE INDEX IF NOT EXISTS idx_create_time ON dwd.eshop_order_sales_dtl (create_time);
CREATE INDEX IF NOT EXISTS idx_entryid ON dwd.eshop_order_sales_dtl (entryid);
CREATE INDEX IF NOT EXISTS idx_customid ON dwd.eshop_order_sales_dtl (customid);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dwd.eshop_order_sales_dtl (goodsid);
CREATE INDEX IF NOT EXISTS idx_product_id ON dwd.eshop_order_sales_dtl (product_id);