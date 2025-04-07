DROP TABLE IF EXISTS dwd.wholesale_contract_doc;
CREATE TABLE dwd.wholesale_contract_doc (
	conid bigint,
    dw_updatetime datetime,
    ysbdjbh bigint COMMENT '药师帮订单编号'
)
UNIQUE KEY(conid) DISTRIBUTED BY HASH(conid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);
INSERT INTO dwd.wholesale_contract_doc (
    conid,
    dw_updatetime,
    ysbdjbh
)
SELECT
	conid,
    dw_updatetime,
    ysbdjbh
FROM
	ods_erp.bms_sa_con_doc 
WHERE is_active = 1;