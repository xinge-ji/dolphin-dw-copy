DROP TABLE IF EXISTS dwd.logistics_warehouse_iwcs_pick;

CREATE TABLE
    dwd.logistics_warehouse_iwcs_pick (
        -- 主键标识
        ssc_picking_carton_id bigint COMMENT '拣货ID',

        -- 时间
        create_time datetime COMMENT '建立时间',
        pick_time datetime COMMENT '拣货时间',
        check_time datetime COMMENT '检查时间',

        -- 关联单据
        wms_inout_id bigint COMMENT 'WMS出入库单据ID,与ssc_picking_carton_id为1对1关系',

        -- 任务类型
        carton_type varchar COMMENT '任务类型:零散出库/整箱出库/托盘出库',

        -- 数量
        whole_qty decimal COMMENT '整件数量',
        scatter_count int COMMENT '散件条目数',
        scatter_qty decimal COMMENT '散件数量'
    ) UNIQUE KEY (ssc_picking_carton_id) DISTRIBUTED BY HASH (ssc_picking_carton_id) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );


INSERT INTO 
    dwd.logistics_warehouse_iwcs_pick (
        ssc_picking_carton_id,
        create_time,
        pick_time,
        check_time,
        wms_inout_id,
        carton_type,
        whole_qty,
        scatter_count,
        scatter_qty
    )
SELECT 
    ssc_picking_carton_id,
    create_date,
    pick_time,
    check_time,
    wms_inout_id,
    CASE 
        WHEN carton_type = 'A' THEN '零散出库'
        WHEN carton_type = 'C' THEN '整箱出库'
        WHEN carton_type = 'P' THEN '托盘出库'
        ELSE '其他'
    END AS carton_type,
    CASE WHEN carton_type = 'A' THEN 0 ELSE allocate_qty / package_num END as whole_qty,
    CASE WHEN carton_type = 'A' THEN 1 ELSE 0 END as scatter_count,
    CASE WHEN carton_type = 'A' THEN allocate_qty ELSE 0 END as scatter_qty
FROM 
    ods_wms.iwcs_ssc_picking_carton;