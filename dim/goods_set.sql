DROP TABLE IF EXISTS dim.goods_set;

CREATE TABLE
    dim.goods_set (
        setdtlid bigint COMMENT '分组明细ID',
        setid bigint COMMENT '分组ID',
        goodsid varchar COMMENT '商品ID'
    ) UNIQUE KEY (setdtlid) DISTRIBUTED BY HASH (setdtlid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO
    dim.goods_set (
        setdtlid,
        setid,
        goodsid
    )
SELECT setdtlid, setid, goodsid
FROM ods_erp.pub_goods_set_dtl
WHERE is_active = 1;

CREATE INDEX IF NOT EXISTS idx_setid ON dim.goods_set (setid);
