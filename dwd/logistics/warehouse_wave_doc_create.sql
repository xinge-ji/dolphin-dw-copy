DROP TABLE IF EXISTS dwd.logistics_warehouse_wave_doc;

CREATE TABLE
    dwd.logistics_warehouse_wave_doc (
        -- 主键标识
        wavedocid bigint COMMENT '波次ID',
        
        -- 时间
        dw_updatetime datetime COMMENT '数据更新时间',
        create_time datetime COMMENT '建立时间',
        execute_time datetime COMMENT '执行时间',

        -- 仓库
        warehid bigint COMMENT '仓库ID',
        warehouse_name varchar COMMENT '仓库名称'
    ) UNIQUE KEY (wavedocid) DISTRIBUTED BY HASH (wavedocid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );


INSERT INTO 
    dwd.logistics_warehouse_wave_doc (
        wavedocid,
        dw_updatetime,
        create_time,
        execute_time,
        warehid,
        warehouse_name
    )
SELECT 
    w.wavedocid,
    w.dw_updatetime,
    w.credate,
    w.executetime,
    w.warehid,
    b.warehname
FROM 
    ods_wms.wms_wave_doc w
LEFT JOIN ods_wms.tpl_warehouse b ON w.warehid = b.warehid AND b.is_active = 1
WHERE 
    w.is_active = 1;

