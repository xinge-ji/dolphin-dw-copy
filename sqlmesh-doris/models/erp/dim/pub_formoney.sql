MODEL (
  name sqlmesh_dim.erp_pub_formoney,
  kind FULL,
  dialect doris,
  grain fmid,
  physical_properties (
    unique_key = (valid_from, valid_to, fmid),
    distributed_by = (kind='HASH', expressions=fmid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
);

@create_dim_wo_create('fmid', 'dw_createtime', '`ods_erp`.`pub_formoney`')