DROP TABLE IF EXISTS dwd.logistics_warehouse_ecode;

CREATE TABLE
    dwd.logistics_warehouse_ecode (
        -- 主键标识
        recordid bigint COMMENT '单据ID',
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 时间
        create_time datetime COMMENT '建立时间',

        -- 关联单据
        sourceid bigint COMMENT '来源单据ID',

        -- 监管码
        ecode bigint COMMENT '监管码'
    ) UNIQUE KEY (recordid, dw_updatetime) DISTRIBUTED BY HASH (recordid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO 
    dwd.logistics_warehouse_ecode (
        recordid,
        dw_updatetime,
        create_time,
        sourceid,
        ecode
    )
SELECT 
    e.recordid,
    e.dw_updatetime AS dw_updatetime,
    e.credate AS create_time,
    e.sourceid,
    e.ecode
FROM 
    ods_wms.wms_ecode_record e
WHERE 
    e.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_recordid ON dwd.logistics_warehouse_ecode (recordid);
CREATE INDEX IF NOT EXISTS idx_sourceid ON dwd.logistics_warehouse_ecode (sourceid);
CREATE INDEX IF NOT EXISTS idx_ecode ON dwd.logistics_warehouse_ecode (ecode);
CREATE INDEX IF NOT EXISTS idx_create_time ON dwd.logistics_warehouse_ecode (create_time);