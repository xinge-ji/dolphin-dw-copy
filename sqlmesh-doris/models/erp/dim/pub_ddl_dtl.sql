MODEL (
  name sqlmesh_dim.erp_pub_ddl_dtl,
  kind FULL,
  dialect doris,
  grain (SYSDTLID as erp_pub_ddl_dtl_id),
  physical_properties (
    unique_key = (valid_from, valid_to, sysdtlid),
    distributed_by = (kind='HASH', expressions=sysdtlid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "数据字典"
);

@create_dim_wo_create('SYSDTLID', 'DW_CREATETIME', '`ods_erp`.`pub_ddl_dtl`')
