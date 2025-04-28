DROP TABLE IF EXISTS dwd.logistics_warehouse_udicode;

CREATE TABLE
    dwd.logistics_warehouse_udicode (
        -- 主键标识
        recordid bigint COMMENT '单据ID',
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 时间
        create_time datetime COMMENT '建立时间',

        -- 关联单据
        sourceid bigint COMMENT '来源单据ID',

        -- udi码
        udicode bigint COMMENT 'udi码'
    ) UNIQUE KEY (recordid, dw_updatetime) DISTRIBUTED BY HASH (recordid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO 
    dwd.logistics_warehouse_udicode (
        recordid,
        dw_updatetime,
        create_time,
        sourceid,
        udicode
    )
SELECT 
    e.recordid,
    e.dw_updatetime AS dw_updatetime,
    e.credate AS create_time,
    e.sourceid,
    e.udicode
FROM 
    ods_wms.wms_udi_code_record e
WHERE 
    e.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_recordid ON dwd.logistics_warehouse_udicode (recordid);
CREATE INDEX IF NOT EXISTS idx_sourceid ON dwd.logistics_warehouse_udicode (sourceid);
CREATE INDEX IF NOT EXISTS idx_udicode ON dwd.logistics_warehouse_udicode (udicode);
CREATE INDEX IF NOT EXISTS idx_create_time ON dwd.logistics_warehouse_udicode (create_time);