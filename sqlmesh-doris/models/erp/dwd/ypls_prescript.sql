MODEL (
  name sqlmesh_dwd.erp_ypls_prescript,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column dw_updatetime,
    lookback 7
  ),
  dialect doris,
  grain (prescriptionid),
  partitioned_by inputdate,
  physical_properties (
    unique_key = (prescriptionid, dw_updatetime),
    distributed_by = (kind='HASH', expressions=prescriptionid, buckets=10),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "YPLS处方表"
);

SELECT
  a.prescriptionid,
  a.dw_updatetime,
  a.is_active,
  a.inputdate,
  a.inputmanid,
  a.memo,
  a.companyid,
  a.prescriptionname,
  a.prescriptionopcode,
  a.usestatus,
  b.employeename,
  c.companyname
FROM ods_erp.ypls_prescript a 
LEFT JOIN sqlmesh_dim.erp_pub_employee b ON a.inputmanid = b.employeeid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_company c ON a.companyid = c.companyid AND c.valid_to = DATE('9999-12-31')
WHERE dw_updatetime BETWEEN @start_ds AND @end_ds;