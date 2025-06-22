DROP TABLE IF EXISTS dws.logistics_warehouse_out_goodsowner_d;
CREATE TABLE dws.logistics_warehouse_out_goodsowner_d(
    -- 颗粒度
    stat_date date COMMENT '统计日期',
    warehid bigint COMMENT '仓库ID',
    goodsownerid bigint COMMENT '货主ID',
    category varchar COMMENT '类别:总数/冷链/中药/其他',

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
    out_whole_qty int COMMENT '整件出库件数'
)
UNIQUE KEY(stat_date, warehid, goodsownerid, category) 
DISTRIBUTED BY HASH(stat_date, warehid, goodsownerid, category) 
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
);

INSERT INTO dws.logistics_warehouse_out_goodsowner_d (
    stat_date,
    warehid,
    goodsownerid,
    category,
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
    out_whole_qty
)
WITH flat_pick_data AS (
    SELECT 
        DATE(io.finish_time) AS stat_date,
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        io.goods_category AS category,
        COUNT(1) AS flat_pick_count,
        SUM(io.whole_qty) as flat_pick_whole_qty,
        SUM(CASE WHEN io.scatter_qty is not NULL THEN 1 ELSE 0 END) AS flat_pick_scatter_count
    FROM 
        dwd.logistics_warehouse_st_io_doc io
    WHERE 
        io.comefrom = 3  -- 来源为波次出库
        AND io.rfmanid != 0  -- 非系统管理员
        AND io.finish_time IS NOT NULL  -- 已完成的单据
        and not exists
        (select 1
                from ods_wms.wms_st_section_def x
                where x.iwcs_flag = 1
                and x.sectionid = io.sectionid
                and x.is_active = 1)
    GROUP BY 
        DATE(io.finish_time),
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        io.goods_category
    
    UNION ALL
    
    SELECT 
        DATE(io.finish_time) AS stat_date,
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        '总数' AS category,
        COUNT(1) AS flat_pick_count,
        SUM(io.whole_qty) as flat_pick_whole_qty,
        SUM(CASE WHEN io.scatter_qty is not NULL THEN 1 ELSE 0 END) AS flat_pick_scatter_count
    FROM 
        dwd.logistics_warehouse_st_io_doc io
    WHERE 
        io.comefrom = 3  -- 来源为波次出库
        AND io.rfmanid != 0  -- 非系统管理员
        AND io.finish_time IS NOT NULL  -- 已完成的单据
        and not exists
        (select 1
                from ods_wms.wms_st_section_def x
                where x.iwcs_flag = 1
                and x.sectionid = io.sectionid
                and x.is_active = 1)
    GROUP BY 
        DATE(io.finish_time),
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name
),
auto_pick_data AS (
    SELECT 
        DATE(p.pick_time) AS stat_date,
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        io.goods_category AS category,
        COUNT(1) as auto_pick_count,
        SUM(p.scatter_count) as auto_pick_scatter_count,
        SUM(p.whole_qty) as auto_pick_whole_qty
    FROM 
        dwd.logistics_warehouse_st_io_doc io
    JOIN
        dwd.logistics_warehouse_iwcs_picking p ON io.inoutid = p.wms_inout_id
    WHERE p.pick_time is not NULL
    GROUP BY 
        DATE(p.pick_time),
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        io.goods_category
    
    UNION ALL
    
    SELECT 
        DATE(p.pick_time) AS stat_date,
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        '总数' AS category,
        COUNT(1) as auto_pick_count,
        SUM(p.scatter_count) as auto_pick_scatter_count,
        SUM(p.whole_qty) as auto_pick_whole_qty
    FROM 
        dwd.logistics_warehouse_st_io_doc io
    JOIN
        dwd.logistics_warehouse_iwcs_picking p ON io.inoutid = p.wms_inout_id
    WHERE p.pick_time is not NULL
    GROUP BY 
        DATE(p.pick_time),
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name
),
auto_udi_data AS (
    SELECT 
        DATE(p.pick_time) AS stat_date,
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        io.goods_category AS category,
        COUNT(1) as auto_udicode_count
    FROM 
        dwd.logistics_warehouse_st_io_doc io
    JOIN
        dwd.logistics_warehouse_iwcs_picking p ON io.inoutid = p.wms_inout_id
    JOIN 
        ods_wms.iwcs_ssc_picking_carton_detail_udi u ON p.wms_inout_id = u.wms_inout_id
    WHERE p.pick_time is not NULL
    GROUP BY 
        DATE(p.pick_time),
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        io.goods_category
    
    UNION ALL
    
    SELECT 
        DATE(p.pick_time) AS stat_date,
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        '总数' AS category,
        COUNT(1) as auto_udicode_count
    FROM 
        dwd.logistics_warehouse_st_io_doc io
    JOIN
        dwd.logistics_warehouse_iwcs_picking p ON io.inoutid = p.wms_inout_id
    JOIN 
        ods_wms.iwcs_ssc_picking_carton_detail_udi u ON p.wms_inout_id = u.wms_inout_id
    WHERE p.pick_time is not NULL
    GROUP BY 
        DATE(p.pick_time),
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name
),
wave_out_data AS (
    SELECT 
        DATE(wd.print_time) AS stat_date,
        wd.warehid,
        wd.goodsownerid,
        wd.warehouse_name,
        wd.goodsowner_name,
        '总数' AS category,
        SUM(wd.out_scatter_box_count) as out_scatter_box_count,
        SUM(wd.out_whole_qty) as out_whole_qty
    FROM 
        dwd.logistics_warehouse_wave_dtl wd
    WHERE 
        wd.print_time IS NOT NULL
    GROUP BY 
        DATE(wd.print_time),
        wd.warehid,
        wd.goodsownerid,
        wd.warehouse_name,
        wd.goodsowner_name
),
-- 汇总所有数据
union_keys AS (
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM flat_pick_data
    UNION
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM auto_pick_data
    UNION
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM auto_udi_data
    UNION
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM wave_out_data
)
SELECT 
    k.stat_date,
    k.warehid,
    k.goodsownerid,
    k.category,
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
    COALESCE(wo.out_whole_qty, 0) as out_whole_qty
FROM union_keys k
LEFT JOIN flat_pick_data fp ON k.stat_date = fp.stat_date AND k.warehid = fp.warehid AND k.goodsownerid = fp.goodsownerid AND k.category = fp.category
LEFT JOIN auto_pick_data ap ON k.stat_date = ap.stat_date AND k.warehid = ap.warehid AND k.goodsownerid = ap.goodsownerid AND k.category = ap.category
LEFT JOIN auto_udi_data au ON k.stat_date = au.stat_date AND k.warehid = au.warehid AND k.goodsownerid = au.goodsownerid AND k.category = au.category
LEFT JOIN wave_out_data wo ON k.stat_date = wo.stat_date AND k.warehid = wo.warehid AND k.goodsownerid = wo.goodsownerid AND k.category = wo.category
WHERE k.warehid is not NULL and k.goodsownerid is not NULL;
