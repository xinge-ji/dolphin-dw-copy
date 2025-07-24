MODEL (
  name sqlmesh_dim.erp_pub_price_type,
  kind FULL,
  dialect doris,
  grain priceid,
  physical_properties (
    unique_key = (valid_from, valid_to, priceid),
    distributed_by = (kind='HASH', expressions=priceid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
);

@create_dim_wo_create('priceid', 'dw_createtime', '`ods_erp`.`pub_price_type`')