MODEL (
  name sqlmesh_dim.erp_pub_country,
  kind FULL,
  dialect doris,
  grain countryid,
  physical_properties (
    unique_key = (valid_from, valid_to, countryid),
    distributed_by = (kind='HASH', expressions=countryid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "区县维度表"
);

@create_dim_wo_create('countryid', 'dw_createtime', '`ods_erp`.`pub_country`')