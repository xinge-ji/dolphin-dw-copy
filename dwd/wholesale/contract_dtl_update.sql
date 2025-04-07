INSERT INTO dwd.wholesale_contract_dtl (conid, condtlid, __DORIS_DELETE_SIGN__)
SELECT a.conid, a.condtlid, 1
FROM ods_erp.bms_sa_con_dtl AS a
JOIN dwd.wholesale_contract_dtl AS b
ON a.conid = b.conid AND a.condtlid = b.condtlid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

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
WHERE b.is_active = 1 AND c.is_active = 1 AND b.dw_updatetime >= (
    SELECT
      max(dw_updatetime) - INTERVAL 60 DAY
    from
      dwd.wholesale_contract_dtl
  );