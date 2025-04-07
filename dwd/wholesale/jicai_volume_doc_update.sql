INSERT INTO dwd.wholesale_jicai_volume_doc (docid, __DORIS_DELETE_SIGN__)
SELECT a.docid, 1
FROM ods_erp.T_101248_DOC AS a
JOIN dwd.wholesale_jicai_volume_doc AS b
ON a.docid = b.docid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO dwd.wholesale_jicai_volume_doc (
    docid,
    goodsid,
    entryid,
    dw_updatetime,
    dlbegindate,
    dlenddate
)
SELECT
	docid,
    goodsid,
    entryid,
    dw_updatetime,
    dlbegindate,
    dlenddate
FROM
	ods_erp.T_101248_DOC
WHERE is_active = 1 AND dw_updatetime >= (
    SELECT
      max(dw_updatetime) - INTERVAL 60 DAY
    from
      dwd.wholesale_jicai_volume_doc
  );