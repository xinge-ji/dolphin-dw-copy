MODEL (
  name sqlmesh_dim.erp_pub_goods_variety_desc_class,
  kind FULL,
  dialect doris,
  grain vardesclassid,
  physical_properties (
    unique_key = (valid_from, valid_to, vardesclassid),
    distributed_by = (kind='HASH', expressions=vardesclassid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
);

@create_dim_wo_create('vardesclassid', 'dw_createtime', '`ods_erp`.`pub_goods_variety_desc_class`')