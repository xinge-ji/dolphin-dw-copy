MODEL (
  name sqlmesh_dim.erp_t_101321,
  kind FULL,
  dialect doris,
  grain goodsid,
  physical_properties (
    unique_key = (valid_from, valid_to, goodsid),
    distributed_by = (kind='HASH', expressions=goodsid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
);

@create_dim_with_create('goodsid', 'credate', 'dw_createtime', '`ods_erp`.`t_101321`')