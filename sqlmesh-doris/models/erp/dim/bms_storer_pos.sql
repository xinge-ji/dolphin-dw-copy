MODEL (
  name sqlmesh_dim.erp_bms_storer_pos,
  kind FULL,
  dialect doris,
  grain (posid),
  physical_properties (
    unique_key = (valid_from, valid_to, posid),
    distributed_by = (kind='HASH', expressions=posid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "货位维度表"
);

WITH ranked_data AS (
    SELECT posid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY posid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    posid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.bms_storer_pos
)
SELECT
  CASE
    WHEN r.record_seq = 1 
    THEN DATE('1970-01-01')
    ELSE date(a.DW_CREATETIME)
  END AS valid_from,
  CASE
    WHEN r.next_start_time IS NOT NULL THEN r.next_start_time
    ELSE DATE('9999-12-31')
  END AS valid_to,
  a.posid,
  a.posno,
  a.sthouseid,
  a.masterid,
  d.opcode AS masteropcode,
  d.employeename AS mastername,
  a.memo,
  a.usestatus,
  a.goodsunitflag,
  pp.placepointid,
  b.sthousename,
  b.storerid,
  c.companyname AS storername,
  a.xpoint,
  a.ypoint,
  a.goodsnum,
  a.lotnum,
  a.existspercent,
  a.existsgoodsnum,
  a.existslotsnum,
  a.posconditions,
  a.posqualitystatus
FROM ranked_data r
JOIN ods_erp.bms_storer_pos a ON r.posid = a.posid AND r.DW_CREATETIME = a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_bms_st_storehouse b ON a.sthouseid = b.sthouseid
LEFT JOIN sqlmesh_dim.erp_pub_employee d ON a.masterid = d.employeeid AND d.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_company c ON b.storerid = c.companyid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_gpcs_placepoint pp ON b.sthouseid = pp.sthouseid AND pp.valid_to = DATE('9999-12-31'); 