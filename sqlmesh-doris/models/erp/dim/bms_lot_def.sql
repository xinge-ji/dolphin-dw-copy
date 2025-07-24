MODEL (
  name sqlmesh_dim.erp_bms_lot_def,
  kind FULL,
  dialect doris,
  grain lotid,
  physical_properties (
    unique_key = (valid_from, valid_to, lotid),
    distributed_by = (kind='HASH', expressions=lotid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "批号维度表"
);

WITH ranked_data AS (
    SELECT lotid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY lotid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    lotid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.bms_lot_def
)
SELECT
  CASE
    WHEN a1.record_seq = 1 
    THEN DATE('1970-01-01')
    ELSE date(a.DW_CREATETIME)
  END AS valid_from,
  CASE
    WHEN a1.next_start_time IS NOT NULL THEN a1.next_start_time
    ELSE DATE('9999-12-31')
  END AS valid_to,
  a.lotid,
  a.lotno,
  a.killlotno,
  a.goodsid,
  b.opcode,
  b.goodsno,
  b.goodsname,
  b.currencyname,
  b.goodstype,
  b.goodsunit,
  b.factoryid,
  c.factoryopcode,
  a.factoryname,
  b.prodarea,
  b.validperiod,
  b.respperiod,
  b.periodunit,
  -- proddate logic
  CASE
    WHEN DATE_FORMAT(a.proddate, '%Y-%m-%d') = '1900-01-01' THEN NULL
    ELSE a.proddate
  END AS proddate,
  -- invaliddate logic
  CASE
    WHEN DATE_FORMAT(a.invaliddate, '%Y-%m-%d') = '2099-01-01' THEN NULL
    ELSE a.invaliddate
  END AS invaliddate,
  a.lotsortno,
  a.approvedocno,
  a.registdocno,
  a.memo,
  a.inputmanid,
  d.employeename AS inputmanname,
  a.credate,
  a.checkno,
  a.checknomemo,
  a.killdate,
  b.accflag,
  b.grade,
  a.printset,
  a.lsh,
  a.zx_price,
  -- printinvdate logic
  CASE
    WHEN a.invaliddate = DATE '2099-01-01' THEN ''
    WHEN a.printset = 1 THEN DATE_FORMAT(a.invaliddate, '%Y-%m')
    ELSE DATE_FORMAT(a.invaliddate, '%Y-%m-%d')
  END AS printinvdate,
  a.zx_ssxkcyr,
  a.invalidflag,
  a.zx_zp_invaliddate
FROM ranked_data a1
JOIN ods_erp.bms_lot_def a ON a1.lotid=a.lotid AND a1.DW_CREATETIME=a.DW_CREATETIME
JOIN sqlmesh_dim.erp_pub_goods b ON a.goodsid = b.goodsid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_factory c ON b.factoryid = c.factoryid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee d ON a.inputmanid = d.employeeid AND d.valid_to = DATE('9999-12-31'); 