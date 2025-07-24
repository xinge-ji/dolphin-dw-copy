MODEL (
  name sqlmesh_dim.erp_gpcs_shop_area_doc,
  kind FULL,
  dialect doris,
  grain areadocid,
  physical_properties (
    unique_key = (valid_from, valid_to, areadocid),
    distributed_by = (kind='HASH', expressions=areadocid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "区县维度表"
);

@create_dim_with_create('areadocid', 'credate', 'dw_createtime', '`ods_erp`.`gpcs_shop_area_doc`')