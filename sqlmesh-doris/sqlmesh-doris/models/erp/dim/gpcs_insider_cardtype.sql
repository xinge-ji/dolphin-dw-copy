MODEL (
  name sqlmesh_dim.erp_gpcs_insider_cardtype,
  kind FULL,
  dialect doris,
  grain inscardtypeid,
  physical_properties (
    unique_key = (valid_from, valid_to, inscardtypeid),
    distributed_by = (kind='HASH', expressions=inscardtypeid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "会员卡类型维度表"
);

WITH ranked_data AS (
    SELECT inscardtypeid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY inscardtypeid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (a.upgratelimit) OVER (ORDER BY a.upgratelimit) AS hilimit,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    inscardtypeid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.gpcs_insider_cardtype a
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
  a.inscardtypeid,
  a.cardopcode,
  a.cardtypename,
  a.rebate,
  a.credate,
  a.usestatus,
  a.inputmanid,
  b.employeename,
  a.priceid,
  c.opcode AS priceopcode,
  c.pricename,
  a.roundmethod,
  a.validperiod,
  a.intprecision,
  a.integralrate,
  a.len,
  a.offsetrate,
  a.upgratelimit,
  a1.hilimit,
  a.cardlevel,
  a.doubtcount,
  a.nosaledayweight,
  a.salenumweight,
  a.realmoneyweight,
  a.maolimoneyweight,
  a.autolocklimit,
  a.autolockflag,
  a.integrebaterate,
  a.svcardflag,
  a.svcardtypeid
FROM ranked_data a1
JOIN ods_erp.gpcs_insider_cardtype a ON a1.inscardtypeid=a.inscardtypeid AND a1.DW_CREATETIME=a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_employee b ON a.inputmanid = b.employeeid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_price_type c ON a.priceid = c.priceid AND c.valid_to = DATE('9999-12-31'); 