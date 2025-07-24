MODEL (
  name sqlmesh_dim.yunuopg_rm_bd_customer,
  kind FULL,
  dialect doris,
  grain (fid as yunuopg_rm_bd_customer_id),
  physical_properties (
    unique_key = (valid_from, valid_to, fid),
    distributed_by = (kind='HASH', expressions=fid, buckets=10),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "客户资料"
);

@create_dim_with_create('fid', 'fcreatetime', 'dw_createtime', '`ods_yunuopg`.`rm_bd_customer`')
