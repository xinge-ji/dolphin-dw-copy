INSERT INTO dwd.wholesale_order_repay_doc (sarecid, __DORIS_DELETE_SIGN__)
SELECT a.sarecid, 1
FROM ods_erp.bms_sa_rec_doc AS a
JOIN dwd.wholesale_order_repay_doc AS b
ON a.sarecid = b.sarecid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO dwd.wholesale_order_repay_doc (
    sarecid,
    dw_updatetime,
    create_date,
    confirm_date,
    shoukuan_type,
    use_status,
    is_yibao_payment
)
SELECT
    sarecid,
    dw_updatetime,
    credate,
    confirmdate,
    CASE 
    	WHEN recmethod = 1 THEN '预收款'
        WHEN recmethod = 2 THEN '现收'
        WHEN recmethod = 3 THEN '收欠款'
        ELSE ''
    END as shoukuan_type,
    CASE
    	WHEN usestatus = 0 THEN '作废'
        WHEN usestatus = 1 THEN '正式'
        WHEN usestatus = 2 THEN '作废'
        ELSE ''
    END AS use_status,
    CASE
    	WHEN zx_ybflag = 1 THEN 1
        ELSE 0
    END AS is_yibao_payment
FROM ods_erp.bms_sa_rec_doc
WHERE is_active = 1 AND dw_updatetime >= (
    SELECT
      max(dw_updatetime) - INTERVAL 60 DAY
    from
      dwd.wholesale_order_repay_doc
  );
