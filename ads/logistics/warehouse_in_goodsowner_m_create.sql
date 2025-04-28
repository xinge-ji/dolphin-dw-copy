DROP TABLE IF EXISTS ads.logistics_warehouse_in_goodsowner_m;
CREATE TABLE ads.logistics_warehouse_in_goodsowner_m(
    -- 颗粒度
    stat_yearmonth date COMMENT '统计年月',
    warehid bigint COMMENT '仓库ID',
    goodsownerid bigint COMMENT '货主ID',

    -- 维度
    warehouse_name varchar COMMENT '仓库名称',
    goodsowner_name varchar COMMENT '货主名称',

    -- 指标
    receive_item_count int COMMENT '收货条目数',
    receive_coldchain_item_count int COMMENT '冷链收货条目数',
    receive_chinesemedicine_item_count int COMMENT '中药收货条目数',
    receive_other_item_count int COMMENT '其他收货条目数',

    check_item_count int COMMENT '验收条目数',
    check_coldchain_item_count int COMMENT '冷链验收条目数',
    check_chinesemedicine_item_count int COMMENT '中药验收条目数',
    check_other_item_count int COMMENT '其他验收条目数',

    flat_shelf_whole_qty decimal COMMENT '平库上架整件件数',
    flat_shelf_coldchain_whole_qty int COMMENT '冷链平库上架整件件数',
    flat_shelf_chinesemedicine_whole_qty int COMMENT '中药平库上架整件件数',
    flat_shelf_other_whole_qty int COMMENT '其他平库上架整件件数',

    flat_shelf_scatter_qty decimal(16, 6) COMMENT '平库上架散件条目数',
    flat_shelf_coldchain_scatter_qty int COMMENT '冷链平库上架散件条目数',
    flat_shelf_chinesemedicine_scatter_qty int COMMENT '中药平库上架散件条目数',
    flat_shelf_other_scatter_qty int COMMENT '其他平库上架散件条目数',

    flat_ecode_count int COMMENT '平库电子监管码',
    flat_coldchain_ecode_count int COMMENT '冷链平库电子监管码',
    flat_chinesemedicine_ecode_count int COMMENT '中药平库电子监管码',
    flat_other_ecode_count int COMMENT '其他平库电子监管码',

    flat_udicode_count int COMMENT '平库UDI码',
    flat_coldchain_udicode_count int COMMENT '冷链平库UDI码',
    flat_chinesemedicine_udicode_count int COMMENT '中药平库UDI码码',
    flat_other_udicode_count int COMMENT '其他平库UDI码',

    auto_shelf_whole_qty decimal COMMENT '立库上架整件件数',
    auto_shelf_coldchain_whole_qty int COMMENT '冷链立库上架整件件数',
    auto_shelf_chinesemedicine_whole_qty int COMMENT '中药立库上架整件件数',
    auto_shelf_other_whole_qty int COMMENT '其他立库上架整件件数',

    auto_shelf_scatter_qty decimal(16, 6) COMMENT '立库上架散件条目数',
    auto_shelf_coldchain_scatter_qty int COMMENT '冷链立库上架散件条目数',
    auto_shelf_chinesemedicine_scatter_qty int COMMENT '中药立库上架散件条目数',
    auto_shelf_other_scatter_qty int COMMENT '其他立库上架散件条目数',

    auto_ecode_count int COMMENT '立库电子监管码',
    auto_coldchain_ecode_count int COMMENT '冷链立库电子监管码',
    auto_chinesemedicine_ecode_count int COMMENT '中药立库电子监管码',
    auto_other_ecode_count int COMMENT '其他立库电子监管码',

    auto_udicode_count int COMMENT '立库UDI码',
    auto_coldchain_udicode_count int COMMENT '冷链立库UDI码',
    auto_chinesemedicine_udicode_count int COMMENT '中药立库UDI码码',
    auto_other_udicode_count int COMMENT '其他立库UDI码'
)
UNIQUE KEY(stat_yearmonth, warehid, goodsownerid) 
DISTRIBUTED BY HASH(stat_yearmonth, warehid, goodsownerid) 
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
);

INSERT INTO ads.logistics_warehouse_in_goodsowner_m (
    -- 颗粒度
    stat_yearmonth,
    warehid,
    goodsownerid,
    
    -- 维度
    warehouse_name,
    goodsowner_name,
    
    -- 指标 - 收货相关
    receive_item_count,
    receive_coldchain_item_count,
    receive_chinesemedicine_item_count,
    receive_other_item_count,
    
    -- 指标 - 验收相关
    check_item_count,
    check_coldchain_item_count,
    check_chinesemedicine_item_count,
    check_other_item_count,
    
    -- 指标 - 平库上架相关
    flat_shelf_whole_qty,
    flat_shelf_coldchain_whole_qty,
    flat_shelf_chinesemedicine_whole_qty,
    flat_shelf_other_whole_qty,
    
    flat_shelf_scatter_qty,
    flat_shelf_coldchain_scatter_qty,
    flat_shelf_chinesemedicine_scatter_qty,
    flat_shelf_other_scatter_qty,
    
    -- 指标 - 立库上架相关
    auto_shelf_whole_qty,
    auto_shelf_coldchain_whole_qty,
    auto_shelf_chinesemedicine_whole_qty,
    auto_shelf_other_whole_qty,
    
    auto_shelf_scatter_qty,
    auto_shelf_coldchain_scatter_qty,
    auto_shelf_chinesemedicine_scatter_qty,
    auto_shelf_other_scatter_qty,

    -- 指标 - 平库电子监管码相关
    flat_ecode_count,
    flat_coldchain_ecode_count,
    flat_chinesemedicine_ecode_count,
    flat_other_ecode_count,

    -- 指标 - 立库电子监管码相关
    auto_ecode_count,
    auto_coldchain_ecode_count,
    auto_chinesemedicine_ecode_count,
    auto_other_ecode_count,

    -- 指标 - 平库UDI码相关
    flat_udicode_count,
    flat_coldchain_udicode_count,
    flat_chinesemedicine_udicode_count,
    flat_other_udicode_count,

    -- 指标 - 立库UDI码相关
    auto_udicode_count,
    auto_coldchain_udicode_count,
    auto_chinesemedicine_udicode_count,
    auto_other_udicode_count
)
SELECT
    -- 颗粒度
    DATE_TRUNC(stat_date,'MONTH') AS stat_yearmonth,
    warehid,
    goodsownerid,
    
    -- 维度
    MAX(warehouse_name) AS warehouse_name,
    MAX(goodsowner_name) AS goodsowner_name,
    
    -- 指标 - 收货相关
    SUM(receive_item_count) AS receive_item_count,
    SUM(receive_coldchain_item_count) AS receive_coldchain_item_count,
    SUM(receive_chinesemedicine_item_count) AS receive_chinesemedicine_item_count,
    SUM(receive_other_item_count) AS receive_other_item_count,
    
    -- 指标 - 验收相关
    SUM(check_item_count) AS check_item_count,
    SUM(check_coldchain_item_count) AS check_coldchain_item_count,
    SUM(check_chinesemedicine_item_count) AS check_chinesemedicine_item_count,
    SUM(check_other_item_count) AS check_other_item_count,
    
    -- 指标 - 平库上架相关
    SUM(flat_shelf_whole_qty) AS flat_shelf_whole_qty,
    SUM(flat_shelf_coldchain_whole_qty) AS flat_shelf_coldchain_whole_qty,
    SUM(flat_shelf_chinesemedicine_whole_qty) AS flat_shelf_chinesemedicine_whole_qty,
    SUM(flat_shelf_other_whole_qty) AS flat_shelf_other_whole_qty,
    
    SUM(flat_shelf_scatter_qty) AS flat_shelf_scatter_qty,
    SUM(flat_shelf_coldchain_scatter_qty) AS flat_shelf_coldchain_scatter_qty,
    SUM(flat_shelf_chinesemedicine_scatter_qty) AS flat_shelf_chinesemedicine_scatter_qty,
    SUM(flat_shelf_other_scatter_qty) AS flat_shelf_other_scatter_qty,
    
    -- 指标 - 立库上架相关
    SUM(auto_shelf_whole_qty) AS auto_shelf_whole_qty,
    SUM(auto_shelf_coldchain_whole_qty) AS auto_shelf_coldchain_whole_qty,
    SUM(auto_shelf_chinesemedicine_whole_qty) AS auto_shelf_chinesemedicine_whole_qty,
    SUM(auto_shelf_other_whole_qty) AS auto_shelf_other_whole_qty,
    
    SUM(auto_shelf_scatter_qty) AS auto_shelf_scatter_qty,
    SUM(auto_shelf_coldchain_scatter_qty) AS auto_shelf_coldchain_scatter_qty,
    SUM(auto_shelf_chinesemedicine_scatter_qty) AS auto_shelf_chinesemedicine_scatter_qty,
    SUM(auto_shelf_other_scatter_qty) AS auto_shelf_other_scatter_qty,

    -- 指标 - 平库电子监管码相关
    SUM(flat_ecode_count) AS flat_ecode_count,
    SUM(flat_coldchain_ecode_count) AS flat_coldchain_ecode_count,
    SUM(flat_chinesemedicine_ecode_count) AS flat_chinesemedicine_ecode_count,
    SUM(flat_other_ecode_count) AS flat_other_ecode_count,

    -- 指标 - 立库电子监管码相关
    SUM(auto_ecode_count) AS auto_ecode_count,
    SUM(auto_coldchain_ecode_count) AS auto_coldchain_ecode_count,
    SUM(auto_chinesemedicine_ecode_count) AS auto_chinesemedicine_ecode_count,
    SUM(auto_other_ecode_count) AS auto_other_ecode_count,

    -- 指标 - 平库UDI码相关
    SUM(flat_udicode_count) AS flat_udicode_count,
    SUM(flat_coldchain_udicode_count) AS flat_coldchain_udicode_count,
    SUM(flat_chinesemedicine_udicode_count) AS flat_chinesemedicine_udicode_count,
    SUM(flat_other_udicode_count) AS flat_other_udicode_count,

    -- 指标 - 立库UDI码相关
    SUM(auto_udicode_count) AS auto_udicode_count,
    SUM(auto_coldchain_udicode_count) AS auto_coldchain_udicode_count,
    SUM(auto_chinesemedicine_udicode_count) AS auto_chinesemedicine_udicode_count,
    SUM(auto_other_udicode_count) AS auto_other_udicode_count
FROM 
    dws.logistics_warehouse_in_goodsowner_d
GROUP BY
    DATE_TRUNC(stat_date,'MONTH'),
    warehid,
    goodsownerid;