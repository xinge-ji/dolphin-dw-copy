DROP TABLE IF EXISTS dim.eshop_customer_buyer;
CREATE TABLE dim.eshop_customer_buyer (
        customid bigint COMMENT '客户ID',
        buyers_id bigint COMMENT '客户id对应电商id'
    ) UNIQUE KEY (customid, buyers_id) DISTRIBUTED BY HASH (customid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO dim.eshop_customer_buyer (
    customid,
    buyers_id
)
SELECT DISTINCT 
    erp_code as customid,
    buyers_id
FROM ods_dsys.sys_buyers;
