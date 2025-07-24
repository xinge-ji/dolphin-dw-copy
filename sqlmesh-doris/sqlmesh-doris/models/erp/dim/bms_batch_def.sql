MODEL (
  name sqlmesh_dim.erp_bms_batch_def,
  kind FULL,
  dialect doris,
  grain batchid,
  physical_properties (
    unique_key = (valid_from, valid_to, batchid),
    distributed_by = (kind='HASH', expressions=batchid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "批次维度表"
);

WITH ranked_data AS (
    SELECT batchid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY batchid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    batchid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.bms_batch_def
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
  a.batchid,
  a.batchno,
  a.batchsortno,
  a.createfrom,
  a.sourceid,
  a.companyid,
  f.companyopcode,
  f.companyname,
  a.goodsid,
  b.opcode,
  b.goodsno,
  b.goodsname,
  b.currencyname,
  b.goodstype,
  b.zx_retainqty,
  b.goodsunit,
  b.factoryid,
  c.factoryopcode,
  c.factoryname,
  b.prodarea,
  b.defaultagtflag,
  a.goodsdtlid,
  d.packname,
  d.packsize,
  a.inputmanid,
  e.employeename AS inputmanname,
  a.credate,
  a.memo,
  a.notaxsuprice,
  a.unitprice,
  a.oldbatchid,
  a.empid,
  g.opcode AS empopcode,
  g.employeename AS empname,
  a.deputyid,
  h.agentname AS deputyname,
  a.limitid,
  i.companyopcode AS limitopcode,
  i.companyname AS limitname,
  a.limitcustomsetid,
  j.setopcode,
  j.setname AS limitcustomset,
  a.taxrate,
  a.qualityinfo,
  k.companyopcode AS bannedCustomerOPCODE,
  k.companyname AS bannedCustomerName,
  l.setopcode AS bannedCustomerSetOpCode,
  l.setname AS bannedCustomerSetName,
  a.bannedcustomerid,
  a.bannedcustomersetid,
  a.zx_batchtype,
  a.zx_lbbz,
  a.zx_jczxflag
FROM ranked_data a1
JOIN ods_erp.bms_batch_def a ON a1.batchid=a.batchid AND a1.DW_CREATETIME=a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_goods b ON a.goodsid = b.goodsid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_factory c ON b.factoryid = c.factoryid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_goods_detail d ON a.goodsdtlid = d.goodsdtlid AND d.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee e ON a.inputmanid = e.employeeid AND e.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_company f ON a.companyid = f.companyid AND f.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee g ON a.empid = g.employeeid AND g.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_bms_agent_def h ON a.deputyid = h.agentid AND h.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_company i ON a.limitid = i.companyid AND i.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_custom_set j ON a.limitcustomsetid = j.setid AND j.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_company k ON a.bannedcustomerid = k.companyid AND k.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_custom_set l ON a.bannedcustomersetid = l.setid AND l.valid_to = DATE('9999-12-31'); 