MODEL (
  name sqlmesh_dim.erp_bms_agent_def,
  kind FULL,
  dialect doris,
  grain agentid,
  physical_properties (
    unique_key = (valid_from, valid_to, agentid),
    distributed_by = (kind='HASH', expressions=agentid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "区县维度表"
);

@create_dim_wo_create('agentid', 'dw_createtime', '`ods_erp`.`bms_agent_def`')