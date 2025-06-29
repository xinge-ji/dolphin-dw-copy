DROP TABLE IF EXISTS dws.logistics_warehouse_out_d;
CREATE TABLE dws.logistics_warehouse_out_d(
    -- 颗粒度
    stat_date date COMMENT '统计日期',
    warehid bigint COMMENT '仓库ID',
    goodsownerid bigint COMMENT '货主ID',
    goods_category varchar COMMENT '类别:冷链/中药/其他',
    operation_type varchar COMMENT '业务类型',
    section_name varchar COMMENT '库区名称',

    -- 描述
    warehouse_name varchar COMMENT '仓库名称',
    goodsowner_name varchar COMMENT '货主名称',

    -- 指标
    flat_pick_count int COMMENT '平库拣货条目数',
    flat_pick_whole_qty int COMMENT '平库拣货整件件数',
    flat_pick_scatter_count int COMMENT '平库拣货散件条目数',
    auto_pick_count int COMMENT '立库拣货条目数',
    auto_pick_whole_qty int COMMENT '立库拣货整件件数',
    auto_pick_scatter_count int COMMENT '立库拣货散件条目数',
    auto_udicode_count int COMMENT '立库UDI码',
    out_scatter_box_count int COMMENT '散件出库箱数',
    out_whole_qty int COMMENT '整件出库件数',

    -- 时间指标
    mean_time_order_to_pick_flat double COMMENT '订单到平库拣货平均用时',
    mean_time_order_to_pick_auto double COMMENT '订单到立库拣货平均用时',   
    mean_time_pick_to_out_flat double COMMENT '平库拣货到出库平均用时',
    mean_time_pick_to_out_auto double COMMENT '立库拣货到出库平均用时'
)
UNIQUE KEY(stat_date, warehid, goodsownerid, goods_category, operation_type, section_name) 
DISTRIBUTED BY HASH(stat_date, warehid) 
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
);

INSERT INTO dws.logistics_warehouse_out_d (
    stat_date,
    warehid,
    goodsownerid,
    goods_category,
    operation_type,
    section_name,
    warehouse_name,
    goodsowner_name,
    flat_pick_count,
    flat_pick_whole_qty,
    flat_pick_scatter_count,
    auto_pick_count,
    auto_pick_whole_qty,
    auto_pick_scatter_count,
    auto_udicode_count,
    out_scatter_box_count,
    out_whole_qty,
    mean_time_order_to_pick_flat,
    mean_time_order_to_pick_auto,
    mean_time_pick_to_out_flat,
    mean_time_pick_to_out_auto
)
WITH flat_pick_data AS (
    SELECT 
        DATE(p.pick_time) AS stat_date,
        p.warehid,
        p.goodsownerid,
        p.goods_category,
        p.operation_type,
        p.section_name,
        p.warehouse_name,
        p.goodsowner_name,
        COUNT(distinct p.pickid) as flat_pick_count,
        SUM(p.whole_qty) as flat_pick_whole_qty,
        SUM(CASE WHEN p.scatter_qty is not null THEN 1 ELSE 0 END) as flat_pick_scatter_count,
        AVG(TIMESTAMPDIFF(MINUTE, p.create_time, p.pick_time)) as mean_time_order_to_pick_flat
    FROM dwd.logistics_warehouse_pick_doc p
    WHERE p.io_comefrom in ('订单出库', '波次出库') 
        AND p.rfmanid != 0  -- 非系统管理员
        AND p.is_iwcs = 0
    GROUP BY 
        DATE(p.pick_time),
        p.warehid,
        p.goodsownerid,
        p.goods_category,
        p.operation_type,
        p.section_name,
        p.warehouse_name,
        p.goodsowner_name
),
auto_pick_data AS (
    SELECT 
        DATE(i.pick_time) AS stat_date,
        p.warehid,
        p.goodsownerid,
        p.goods_category,
        p.operation_type,
        p.section_name,
        p.warehouse_name,
        p.goodsowner_name,
        COUNT(distinct i.ssc_picking_carton_id) as auto_pick_count,
        SUM(i.whole_qty) as auto_pick_whole_qty,
        SUM(i.scatter_count) as auto_pick_scatter_count,
        AVG(TIMESTAMPDIFF(MINUTE, p.create_time, i.pick_time)) as mean_time_order_to_pick_auto
    FROM 
        dwd.logistics_warehouse_pick_doc p
    JOIN
        dwd.logistics_warehouse_iwcs_pick i ON p.inoutid = i.wms_inout_id
    WHERE p.io_comefrom in ('订单出库', '波次出库') 
    GROUP BY 
        DATE(i.pick_time),
        p.warehid,
        p.goodsownerid,
        p.goods_category,
        p.operation_type,
        p.section_name,
        p.warehouse_name,
        p.goodsowner_name
),
auto_udi_data AS (
    SELECT 
        DATE(i.pick_time) AS stat_date,
        p.warehid,
        p.goodsownerid,
        p.goods_category,
        p.operation_type,
        p.section_name,
        p.warehouse_name,
        p.goodsowner_name,
        COUNT(distinct u.ssc_picking_carton_detail_id) as auto_udicode_count
    FROM 
        dwd.logistics_warehouse_pick_doc p
    JOIN
        dwd.logistics_warehouse_iwcs_pick i ON p.inoutid = i.wms_inout_id
    JOIN 
        ods_wms.iwcs_ssc_picking_carton_detail_udi u ON i.wms_inout_id = u.wms_inout_id
    WHERE p.io_comefrom in ('订单出库', '波次出库') 
    GROUP BY 
        DATE(i.pick_time),
        p.warehid,
        p.goodsownerid,
        p.goods_category,
        p.operation_type,
        p.section_name,
        p.warehouse_name,
        p.goodsowner_name
),
last_pick_data AS (
    SELECT 
        p.sourceid,
        MAX(p.warehid) as warehid,
        MAX(p.goodsownerid) as goodsownerid,
        MAX(p.goods_category) as goods_category,
        MAX(p.operation_type) as operation_type,
        MAX(p.section_name) as section_name,
        MAX(p.warehouse_name) as warehouse_name,
        0 as is_iwcs,
        MAX(p.pick_time) as pick_time
    FROM dwd.logistics_warehouse_pick_doc p
    WHERE p.io_comefrom in ('订单出库', '波次出库') 
    AND p.rfmanid != 0  -- 非系统管理员
    GROUP BY 
        p.sourceid
    
    UNION ALL
    
    SELECT 
        p.sourceid,
        MAX(p.warehid) as warehid,
        MAX(p.goodsownerid) as goodsownerid,
        MAX(p.goods_category) as goods_category,
        MAX(p.operation_type) as operation_type,
        MAX(p.section_name) as section_name,
        MAX(p.warehouse_name) as warehouse_name,
        1 as is_iwcs,
        MAX(i.pick_time) as pick_time
    FROM dwd.logistics_warehouse_pick_doc p
    JOIN
        dwd.logistics_warehouse_iwcs_pick i ON p.inoutid = i.wms_inout_id
    WHERE p.io_comefrom in ('订单出库', '波次出库') 
    GROUP BY 
        p.sourceid
),
wave_out_data AS (
    SELECT 
        DATE(w.print_time) AS stat_date,
        w.warehid,
        w.goodsownerid,
        p.goods_category,
        p.operation_type,
        p.section_name,
        w.warehouse_name,
        w.goodsowner_name,
        SUM(w.out_scatter_box_count) as out_scatter_box_count,
        SUM(w.out_whole_qty) as out_whole_qty,
        AVG(TIMESTAMPDIFF(MINUTE, CASE WHEN p.is_iwcs = 0 THEN p.pick_time ELSE NULL END, w.print_time)) as mean_time_pick_to_out_flat,
        AVG(TIMESTAMPDIFF(MINUTE, CASE WHEN p.is_iwcs = 1 THEN p.pick_time ELSE NULL END, w.print_time)) as mean_time_pick_to_out_auto
    FROM dwd.logistics_warehouse_wave_dtl w
    JOIN last_pick_data p ON w.wavedtlid = p.sourceid
    WHERE w.print_time IS NOT NULL
    GROUP BY 
        DATE(w.print_time),
        w.warehid,
        w.goodsownerid,
        p.goods_category,
        p.operation_type,
        p.section_name,
        w.warehouse_name,
        w.goodsowner_name
),
union_keys AS (
    SELECT stat_date, warehid, goodsownerid, goods_category, operation_type, section_name, warehouse_name, goodsowner_name FROM flat_pick_data
    UNION
    SELECT stat_date, warehid, goodsownerid, goods_category, operation_type, section_name, warehouse_name, goodsowner_name FROM auto_pick_data
    UNION
    SELECT stat_date, warehid, goodsownerid, goods_category, operation_type, section_name, warehouse_name, goodsowner_name FROM auto_udi_data
    UNION
    SELECT stat_date, warehid, goodsownerid, goods_category, operation_type, section_name, warehouse_name, goodsowner_name FROM wave_out_data
)
SELECT
    k.stat_date,
    k.warehid,
    k.goodsownerid,
    k.goods_category,
    k.operation_type,
    k.section_name,
    k.warehouse_name,
    k.goodsowner_name,
    COALESCE(fp.flat_pick_count, 0) as flat_pick_count,
    COALESCE(fp.flat_pick_whole_qty, 0) as flat_pick_whole_qty,
    COALESCE(fp.flat_pick_scatter_count, 0) as flat_pick_scatter_count,
    COALESCE(ap.auto_pick_count, 0) as auto_pick_count,
    COALESCE(ap.auto_pick_whole_qty, 0) as auto_pick_whole_qty,
    COALESCE(ap.auto_pick_scatter_count, 0) as auto_pick_scatter_count,
    COALESCE(au.auto_udicode_count, 0) as auto_udicode_count,
    COALESCE(wo.out_scatter_box_count, 0) as out_scatter_box_count,
    COALESCE(wo.out_whole_qty, 0) as out_whole_qty,
    COALESCE(fp.mean_time_order_to_pick_flat, 0) as mean_time_order_to_pick_flat,
    COALESCE(ap.mean_time_order_to_pick_auto, 0) as mean_time_order_to_pick_auto,
    COALESCE(wo.mean_time_pick_to_out_flat, 0) as mean_time_pick_to_out_flat,
    COALESCE(wo.mean_time_pick_to_out_auto, 0) as mean_time_pick_to_out_auto
FROM union_keys k
LEFT JOIN flat_pick_data fp ON k.stat_date = fp.stat_date AND k.warehid = fp.warehid AND k.goodsownerid = fp.goodsownerid AND k.goods_category = fp.goods_category AND k.operation_type = fp.operation_type AND k.section_name = fp.section_name AND k.warehouse_name = fp.warehouse_name AND k.goodsowner_name = fp.goodsowner_name
LEFT JOIN auto_pick_data ap ON k.stat_date = ap.stat_date AND k.warehid = ap.warehid AND k.goodsownerid = ap.goodsownerid AND k.goods_category = ap.goods_category AND k.operation_type = ap.operation_type AND k.section_name = ap.section_name AND k.warehouse_name = ap.warehouse_name AND k.goodsowner_name = ap.goodsowner_name
LEFT JOIN auto_udi_data au ON k.stat_date = au.stat_date AND k.warehid = au.warehid AND k.goodsownerid = au.goodsownerid AND k.goods_category = au.goods_category AND k.operation_type = au.operation_type AND k.section_name = au.section_name AND k.warehouse_name = au.warehouse_name AND k.goodsowner_name = au.goodsowner_name
LEFT JOIN wave_out_data wo ON k.stat_date = wo.stat_date AND k.warehid = wo.warehid AND k.goodsownerid = wo.goodsownerid AND k.goods_category = wo.goods_category AND k.operation_type = wo.operation_type AND k.section_name = wo.section_name AND k.warehouse_name = wo.warehouse_name AND k.goodsowner_name = wo.goodsowner_name;
