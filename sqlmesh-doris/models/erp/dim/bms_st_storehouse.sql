MODEL (
  name sqlmesh_dim.erp_bms_st_storehouse,
  kind FULL,
  dialect doris,
  grain STHOUSEID,
  physical_properties (
    unique_key = (valid_from, valid_to, STHOUSEID),
    distributed_by = (kind='HASH', expressions=STHOUSEID, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "区县维度表"
);

@create_dim_wo_create('STHOUSEID', 'dw_createtime', '`ods_erp`.`bms_st_storehouse`')