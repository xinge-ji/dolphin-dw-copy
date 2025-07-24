MODEL (
  name sqlmesh_dim.erp_zx_pub_taxrate,
  kind FULL,
  dialect doris,
  grain (seqid as erp_zx_pub_taxrate_id),
  physical_properties (
    unique_key = (valid_from, valid_to, seqid),
    distributed_by = (kind='HASH', expressions=seqid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "税率主数据宽表"
);

WITH ranked_data AS (
    SELECT seqid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY seqid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    seqid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.zx_pub_taxrate
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
  a.seqid,
  a.goodsid,
  f.goodsname,
  f.opcode,
  f.goodsunit,
  f.goodstype,
  f.prodarea,
  f.factoryid,
  g.factoryname,
  a.entryid,
  d.entryname,
  a.supplyid,
  b.supplyopcode,
  b.supplyname,
  a.supplytaxrate,
  a.customid,
  c.customopcode,
  c.customname,
  a.salestaxrate,
  a.inputmanid,
  e.employeename AS inputman,
  a.credate,
  a.kaypjyzsbs,
  a.kaypjyzsksyf,
  f.currencyname
FROM ranked_data a1
JOIN ods_erp.zx_pub_taxrate a ON a1.seqid=a.seqid AND a1.DW_CREATETIME=a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_supplyer b ON a.supplyid = b.supplyid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_customer c ON a.customid = c.customid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_entry d ON a.entryid = d.entryid AND d.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee e ON a.inputmanid = e.employeeid AND e.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_goods f ON a.goodsid = f.goodsid AND f.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_factory g ON f.factoryid = g.factoryid AND g.valid_to = DATE('9999-12-31')