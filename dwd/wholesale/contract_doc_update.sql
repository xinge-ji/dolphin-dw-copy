INSERT INTO dwd.wholesale_contract_doc (conid, __DORIS_DELETE_SIGN__)
SELECT a.conid, 1
FROM ods_erp.bms_sa_con_doc AS a
JOIN dwd.wholesale_contract_doc AS b
ON a.conid = b.conid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

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
WHERE is_active = 1 AND dw_updatetime >= (
    SELECT
      max(dw_updatetime) - INTERVAL 60 DAY
    from
      dwd.wholesale_contract_doc
  )