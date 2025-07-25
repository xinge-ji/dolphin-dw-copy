DROP TABLE IF EXISTS dws.logistics_warehouse_in_d;
CREATE TABLE dws.logistics_warehouse_in_d(
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
    receive_count int COMMENT '收货条目数',
    check_count int COMMENT '验收条目数',
    check_scatter_count int COMMENT '散件验收条目数',
    check_whole_count int COMMENT '整件验收条目数',
    check_scatter_qty decimal(16,6) COMMENT '散件验收件数',
    check_whole_qty decimal COMMENT '整件验收件数',
    flat_shelf_count int COMMENT '平库上架条目数',
    flat_shelf_whole_qty decimal COMMENT '平库上架整件件数',
    flat_shelf_scatter_count int COMMENT '平库上架散件条目数',
    auto_shelf_count int COMMENT '立库上架条目数',
    auto_shelf_whole_qty decimal COMMENT '立库上架整件件数',
    auto_shelf_scatter_count int COMMENT '立库上架散件条目数',
    ecode_count int COMMENT '电子监管码',
    udicode_count int COMMENT 'UDI码',

    -- 时间指标
    mean_time_order_to_receive double COMMENT '订单到收货平均用时',
    mean_time_receive_to_check double COMMENT '收货到验收平均用时',
    mean_time_check_to_flat double COMMENT '验到平库上架平均用时',
    mean_time_check_to_auto double COMMENT '验到立库上架平均用时',

    -- 时效指标
    order_to_receive_2d_count int COMMENT '2天订单到收货完成条目数',
    order_to_receive_7d_count int COMMENT '7天订单到收货完成条目数',
    receive_to_check_24h_count int COMMENT '24小时收货到验收完成条目数',
    receive_to_check_48h_count int COMMENT '48小时收货到验货完成条目数',
    check_to_flat_24h_count int COMMENT '24小时验收上架平库完成条目数',
    check_to_flat_48h_count int COMMENT '48小时验收上架平库完成条目数',
    check_to_auto_24h_count int COMMENT '24小时验收上架立库完成条目数',
    check_to_auto_48h_count int COMMENT '48小时验收上架立库完成条目数'
)
UNIQUE KEY (stat_date, warehid, goodsownerid, goods_category, operation_type, section_name)
DISTRIBUTED BY HASH(stat_date, warehid)
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
);

INSERT INTO dws.logistics_warehouse_in_d (
    stat_date,
    warehid,
    goodsownerid,
    goods_category,
    operation_type,
    section_name,
    warehouse_name,
    goodsowner_name,
    receive_count,
    check_count,
    check_scatter_count,
    check_whole_count,
    check_scatter_qty,
    check_whole_qty,
    flat_shelf_count,
    flat_shelf_whole_qty,
    flat_shelf_scatter_count,
    auto_shelf_count,
    auto_shelf_whole_qty,
    auto_shelf_scatter_count,
    ecode_count,
    udicode_count,
    mean_time_order_to_receive,
    mean_time_receive_to_check,
    mean_time_check_to_flat,
    mean_time_check_to_auto,
    order_to_receive_2d_count,
    order_to_receive_7d_count,
    receive_to_check_24h_count,
    receive_to_check_48h_count,
    check_to_flat_24h_count,
    check_to_flat_48h_count,
    check_to_auto_24h_count,
    check_to_auto_48h_count
)
WITH filtered_in_dtl AS (
    -- 先过滤需要的入库明细数据
    SELECT 
        a.indtlid, a.receive_time, a.warehid, a.goodsownerid, 
        a.goods_category, a.operation_type, a.warehouse_name, 
        a.goodsowner_name, a.inid, a.order_to_receive_time
    FROM dwd.logistics_warehouse_order_in_dtl a
    JOIN dwd.logistics_warehouse_order_in_doc doc ON a.inid = doc.inid
    WHERE a.is_recheck = 0 AND doc.is_autotask = 0
),
section_info_indtlid AS (
    -- 使用稳定的ORDER BY条件，避免ROW_NUMBER不确定性导致的结果波动
    SELECT 
        rd.indtlid,
        sd.section_name,
        ROW_NUMBER() OVER (
            PARTITION BY rd.indtlid 
            ORDER BY sd.check_time DESC,     -- 主排序：最新验收时间
                     sd.section_name ASC,    -- 次排序：section_name保证一致性
                     sd.receiveid ASC        -- 第三排序：receiveid确保唯一性
        ) as rn
    FROM filtered_in_dtl rd
    JOIN dwd.logistics_warehouse_order_receive_dtl sd ON sd.indtlid = rd.indtlid
),
-- 入库明细基础数据（聚合）
receive_base AS (
    SELECT 
        DATE(a.receive_time) as stat_date,
        a.warehid, a.goodsownerid, a.goods_category, a.operation_type,
        a.warehouse_name, a.goodsowner_name,
        COALESCE(si.section_name, '其他') as section_name,
        -- 聚合指标
        COUNT(DISTINCT a.indtlid) as receive_count,
        AVG(a.order_to_receive_time) as mean_time_order_to_receive,
        -- 时效指标
        SUM(CASE WHEN a.order_to_receive_time <= 2 THEN 1 ELSE 0 END) as order_to_receive_2d_count,
        SUM(CASE WHEN a.order_to_receive_time <= 7 THEN 1 ELSE 0 END) as order_to_receive_7d_count
    FROM filtered_in_dtl a
    LEFT JOIN (SELECT indtlid, section_name FROM section_info_indtlid WHERE rn = 1) si ON a.indtlid = si.indtlid
    GROUP BY 
        DATE(a.receive_time), a.warehid, a.goodsownerid, a.goods_category,
        a.operation_type, a.warehouse_name, a.goodsowner_name,
        COALESCE(si.section_name, '其他')
),

-- 验收明细数据（聚合）
check_detail AS (
    SELECT 
        DATE(b.check_time) as stat_date,
        f.warehid, f.goodsownerid, f.goods_category, f.operation_type,
        f.warehouse_name, f.goodsowner_name, b.section_name, 
        -- 聚合指标
        COUNT(DISTINCT b.receiveid) as check_count,
        SUM(CASE WHEN b.scatter_qty != 0 THEN 1 ELSE 0 END) as check_scatter_count,
        SUM(CASE WHEN b.whole_qty != 0 THEN 1 ELSE 0 END) as check_whole_count,
        SUM(b.scatter_qty) as check_scatter_qty,
        SUM(b.whole_qty) as check_whole_qty,
        AVG(TIMESTAMPDIFF(MINUTE, f.receive_time, b.check_time)) as mean_time_receive_to_check,
        -- 时效指标
        SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, f.receive_time, b.check_time) <= 1440 THEN 1 ELSE 0 END) as receive_to_check_24h_count,
        SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, f.receive_time, b.check_time) <= 2880 THEN 1 ELSE 0 END) as receive_to_check_48h_count
    FROM dwd.logistics_warehouse_order_receive_dtl b
    JOIN filtered_in_dtl f ON b.indtlid = f.indtlid
    WHERE b.check_time IS NOT NULL
    GROUP BY 
        DATE(b.check_time), f.warehid, f.goodsownerid, f.goods_category,
        f.operation_type, f.warehouse_name, f.goodsowner_name,
        b.section_name
),

-- 平库上架明细数据（聚合）
flat_shelf_detail AS (
    SELECT 
        DATE(c.shelf_time) as stat_date,
        f.warehid, f.goodsownerid, f.goods_category, f.operation_type,
        f.warehouse_name, f.goodsowner_name, c.section_name,
        -- 聚合指标
        COUNT(DISTINCT c.shelfid) as flat_shelf_count,
        IFNULL(SUM(c.whole_qty), 0) as flat_shelf_whole_qty,
        SUM(CASE WHEN c.scatter_qty != 0 THEN 1 ELSE 0 END) as flat_shelf_scatter_count,
        AVG(TIMESTAMPDIFF(MINUTE, b.check_time, c.shelf_time)) as mean_time_check_to_flat,
        -- 时效指标
        SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, b.check_time, c.shelf_time) <= 1440 THEN 1 ELSE 0 END) as check_to_flat_24h_count,
        SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, b.check_time, c.shelf_time) <= 2880 THEN 1 ELSE 0 END) as check_to_flat_48h_count
    FROM dwd.logistics_warehouse_shelf_doc c
    JOIN dwd.logistics_warehouse_order_receive_dtl b ON b.receiveid = c.sourceid
    JOIN filtered_in_dtl f ON b.indtlid = f.indtlid
    WHERE c.shelf_time IS NOT NULL
        AND c.rfmanid != 0
        AND c.is_iwcs != 1  -- 平库数据
    GROUP BY 
        DATE(c.shelf_time), f.warehid, f.goodsownerid, f.goods_category,
        f.operation_type, f.warehouse_name, f.goodsowner_name, c.section_name
),

-- 立库上架明细数据（聚合）
auto_shelf_detail AS (
    SELECT 
        DATE(d.create_time) as stat_date,
        f.warehid, f.goodsownerid, f.goods_category, f.operation_type,
        f.warehouse_name, f.goodsowner_name, b.section_name,
        -- 聚合指标
        COUNT(DISTINCT d.ssc_receive_goods_locate_id) as auto_shelf_count,
        IFNULL(SUM(d.whole_qty), 0) as auto_shelf_whole_qty,
        IFNULL(SUM(d.scatter_count), 0) as auto_shelf_scatter_count,
        AVG(TIMESTAMPDIFF(MINUTE, b.check_time, d.create_time)) as mean_time_check_to_auto,
        -- 时效指标
        SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, b.check_time, d.create_time) <= 1440 THEN 1 ELSE 0 END) as check_to_auto_24h_count,
        SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, b.check_time, d.create_time) <= 2880 THEN 1 ELSE 0 END) as check_to_auto_48h_count
    FROM dwd.logistics_warehouse_iwcs_shelf d
    JOIN dwd.logistics_warehouse_order_receive_dtl b ON b.receiveid = d.receiveid
    JOIN filtered_in_dtl f ON b.indtlid = f.indtlid
    WHERE d.create_time IS NOT NULL
    GROUP BY 
        DATE(d.create_time), f.warehid, f.goodsownerid, f.goods_category, f.operation_type,
        f.warehouse_name, f.goodsowner_name, b.section_name
),

-- 电子监管码数据（聚合）
ecode_detail AS (
    SELECT 
        DATE(e.create_time) as stat_date,
        f.warehid, f.goodsownerid, f.goods_category, f.operation_type,
        f.warehouse_name, f.goodsowner_name, s.section_name,
        COUNT(e.recordid) as ecode_count
    FROM dwd.logistics_warehouse_ecode e
    JOIN filtered_in_dtl f ON e.sourceid = f.indtlid
    JOIN dwd.logistics_warehouse_shelf_doc s ON s.sourceid = e.sourceid
    WHERE e.create_time IS NOT NULL
    GROUP BY 
        DATE(e.create_time), f.warehid, f.goodsownerid, f.goods_category, f.operation_type,
        f.warehouse_name, f.goodsowner_name, s.section_name
),

-- UDI码数据（聚合）
udicode_detail AS (
    SELECT 
        DATE(e.create_time) as stat_date,
        f.warehid, f.goodsownerid, f.goods_category, f.operation_type,
        f.warehouse_name, f.goodsowner_name, s.section_name,
        COUNT(e.recordid) as udicode_count
    FROM dwd.logistics_warehouse_udicode e
    JOIN filtered_in_dtl f ON e.sourceid = f.indtlid
    JOIN dwd.logistics_warehouse_shelf_doc s ON s.sourceid = e.sourceid
    WHERE e.create_time IS NOT NULL
    GROUP BY 
        DATE(e.create_time), f.warehid, f.goodsownerid, f.goods_category, f.operation_type,
        f.warehouse_name, f.goodsowner_name, s.section_name
),

-- 汇总所有维度组合
all_dimensions AS (
    SELECT distinct stat_date, warehid, goodsownerid, goods_category, operation_type, section_name, warehouse_name, goodsowner_name FROM receive_base
    UNION
    SELECT distinct stat_date, warehid, goodsownerid, goods_category, operation_type, section_name, warehouse_name, goodsowner_name FROM check_detail
    UNION
    SELECT distinct stat_date, warehid, goodsownerid, goods_category, operation_type, section_name, warehouse_name, goodsowner_name FROM flat_shelf_detail
    UNION
    SELECT distinct stat_date, warehid, goodsownerid, goods_category, operation_type, section_name, warehouse_name, goodsowner_name FROM auto_shelf_detail
    UNION
    SELECT distinct stat_date, warehid, goodsownerid, goods_category, operation_type, section_name, warehouse_name, goodsowner_name FROM ecode_detail
    UNION
    SELECT distinct stat_date, warehid, goodsownerid, goods_category, operation_type, section_name, warehouse_name, goodsowner_name FROM udicode_detail
)

-- 最终聚合查询
SELECT 
    ad.stat_date,
    ad.warehid,
    ad.goodsownerid,
    ad.goods_category,
    ad.operation_type,
    ad.section_name,
    ad.warehouse_name,
    ad.goodsowner_name,
    
    -- 收货指标
    COALESCE(rb.receive_count, 0) as receive_count,
    
    -- 验收指标
    COALESCE(cd.check_count, 0) as check_count,
    COALESCE(cd.check_scatter_count, 0) as check_scatter_count,
    COALESCE(cd.check_whole_count, 0) as check_whole_count,
    COALESCE(cd.check_scatter_qty, 0) as check_scatter_qty,
    COALESCE(cd.check_whole_qty, 0) as check_whole_qty,
    
    -- 平库上架指标
    COALESCE(fsd.flat_shelf_count, 0) as flat_shelf_count,
    COALESCE(fsd.flat_shelf_whole_qty, 0) as flat_shelf_whole_qty, 
    COALESCE(fsd.flat_shelf_scatter_count, 0) as flat_shelf_scatter_count,
    
    -- 立库上架指标
    COALESCE(asd.auto_shelf_count, 0) as auto_shelf_count,
    COALESCE(asd.auto_shelf_whole_qty, 0) as auto_shelf_whole_qty, 
    COALESCE(asd.auto_shelf_scatter_count, 0) as auto_shelf_scatter_count, 
    
    -- 码类指标
    COALESCE(ed.ecode_count, 0) as ecode_count,
    COALESCE(ud.udicode_count, 0) as udicode_count,

    -- 时间
    COALESCE(rb.mean_time_order_to_receive, 0) as mean_time_order_to_receive,
    COALESCE(cd.mean_time_receive_to_check, 0) as mean_time_receive_to_check,
    COALESCE(fsd.mean_time_check_to_flat, 0) as mean_time_check_to_flat,
    COALESCE(asd.mean_time_check_to_auto, 0) as mean_time_check_to_auto,
    
    -- 时效指标
    COALESCE(rb.order_to_receive_2d_count, 0) as order_to_receive_2d_count,
    COALESCE(rb.order_to_receive_7d_count, 0) as order_to_receive_7d_count,
    COALESCE(cd.receive_to_check_24h_count, 0) as receive_to_check_24h_count,
    COALESCE(cd.receive_to_check_48h_count, 0) as receive_to_check_48h_count,
    COALESCE(fsd.check_to_flat_24h_count, 0) as check_to_flat_24h_count,
    COALESCE(fsd.check_to_flat_48h_count, 0) as check_to_flat_48h_count,
    COALESCE(asd.check_to_auto_24h_count, 0) as check_to_auto_24h_count,
    COALESCE(asd.check_to_auto_48h_count, 0) as check_to_auto_48h_count
    
FROM all_dimensions ad
LEFT JOIN receive_base rb ON ad.stat_date = rb.stat_date 
    AND ad.warehid = rb.warehid 
    AND ad.goodsownerid = rb.goodsownerid 
    AND ad.goods_category = rb.goods_category 
    AND ad.operation_type = rb.operation_type 
    AND ad.section_name = rb.section_name
LEFT JOIN check_detail cd ON ad.stat_date = cd.stat_date 
    AND ad.warehid = cd.warehid 
    AND ad.goodsownerid = cd.goodsownerid 
    AND ad.goods_category = cd.goods_category 
    AND ad.operation_type = cd.operation_type 
    AND ad.section_name = cd.section_name
LEFT JOIN flat_shelf_detail fsd ON ad.stat_date = fsd.stat_date 
    AND ad.warehid = fsd.warehid 
    AND ad.goodsownerid = fsd.goodsownerid 
    AND ad.goods_category = fsd.goods_category 
    AND ad.operation_type = fsd.operation_type 
    AND ad.section_name = fsd.section_name
LEFT JOIN auto_shelf_detail asd ON ad.stat_date = asd.stat_date 
    AND ad.warehid = asd.warehid 
    AND ad.goodsownerid = asd.goodsownerid 
    AND ad.goods_category = asd.goods_category 
    AND ad.operation_type = asd.operation_type 
    AND ad.section_name = asd.section_name
LEFT JOIN ecode_detail ed ON ad.stat_date = ed.stat_date 
    AND ad.warehid = ed.warehid 
    AND ad.goodsownerid = ed.goodsownerid 
    AND ad.goods_category = ed.goods_category 
    AND ad.operation_type = ed.operation_type 
    AND ad.section_name = ed.section_name
LEFT JOIN udicode_detail ud ON ad.stat_date = ud.stat_date 
    AND ad.warehid = ud.warehid 
    AND ad.goodsownerid = ud.goodsownerid 
    AND ad.goods_category = ud.goods_category 
    AND ad.operation_type = ud.operation_type 
    AND ad.section_name = ud.section_name;