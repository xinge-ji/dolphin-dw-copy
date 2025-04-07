DROP TABLE IF EXISTS dwd.wholesale_contract_dtl;
CREATE TABLE dwd.wholesale_contract_dtl (
	conid bigint,
    condtlid bigint,
    dw_updatetime datetime,
    ysbdjbh bigint COMMENT '药师帮订单编号',
    salesdtlid bigint
)
UNIQUE KEY(conid, condtlid) DISTRIBUTED BY HASH(conid, condtlid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);
INSERT INTO dwd.wholesale_contract_dtl (
    conid,
    condtlid,
    dw_updatetime,
    ysbdjbh,
    salesdtlid
)
SELECT
	a.conid,
    b.condtlid,
    b.dw_updatetime,
    a.ysbdjbh,
    c.salesdtlid
FROM
	dwd.wholesale_contract_doc a
JOIN
	ods_erp.bms_sa_con_dtl b ON a.conid = b.conid
LEFT JOIN
	ods_erp.BMS_SA_CONTODOC c ON b.condtlid = c.condtlid
WHERE b.is_active = 1 AND c.is_active = 1;