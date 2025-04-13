DROP TABLE IF EXISTS dim.customer_set;

CREATE TABLE
    dim.customer_set (
        setdtlid bigint COMMENT '分组明细ID',
        setid bigint COMMENT '分组ID',
        customid varchar COMMENT '客户ID'
    ) UNIQUE KEY (setdtlid) DISTRIBUTED BY HASH (setdtlid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO
    dim.customer_set (
        setdtlid,
        setid,
        customid
    )
SELECT setdtlid, setid, customid
FROM ods_erp.pub_custom_set_dtl
WHERE is_active = 1;

CREATE INDEX IF NOT EXISTS idx_setid ON dim.customer_set (setid);
