DROP TABLE IF EXISTS dim.eshop_entry_goods;
CREATE TABLE dim.eshop_entry_goods (
        entryid bigint COMMENT '独立单元ID',
        goodsid bigint COMMENT '客户ID',
        dw_starttime datetime COMMENT '商品上线日期',
        dw_endtime datetime COMMENT '商品下线日期',
        org_id bigint COMMENT '独立单元id对应电商id',
        product_id bigint COMMENT '商品id对应电商id'
    ) UNIQUE KEY (entryid, goodsid, dw_starttime) DISTRIBUTED BY HASH (goodsid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );


-- 插入数据到dim.eshop_entry_goods表
INSERT INTO dim.eshop_entry_goods (
    entryid,
    goodsid,
    product_id,
    org_id,
    dw_starttime,
    dw_endtime
)
SELECT 
        t1.entryid,
        t1.goodsid,
        MAX(t2.product_id),
        MAX(t2.org_id),
        MIN(IFNULL(t1.credate, t2.create_time)) AS dw_starttime,
        CAST('9999-12-31 23:59:59' AS DATETIME) AS dw_endtime
    FROM ods_erp.eshop_entry_goods t1
    LEFT JOIN (SELECT entryid, MAX(org_id) as org_id FROM dim.entry group by entryid) e ON t1.entryid = e.entryid
    LEFT JOIN ods_dsys.prd_product t2 on t1.goodsid = t2.PRODUCT_CODE AND e.org_id = t2.org_id
    WHERE t1.is_active = 1 and t1.entryid is not null and t1.goodsid is not null
    GROUP BY t1.entryid, t1.goodsid;


CREATE INDEX IF NOT EXISTS idx_entryid ON dim.eshop_entry_goods (entryid);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dim.eshop_entry_goods (goodsid);
CREATE INDEX IF NOT EXISTS idx_product_id ON dim.eshop_entry_goods (product_id);
CREATE INDEX IF NOT EXISTS idx_org_id ON dim.eshop_entry_goods (org_id);
