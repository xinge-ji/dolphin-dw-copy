DROP TABLE IF EXISTS dwd.logistics_warehouse_order_in_doc;

CREATE TABLE
    dwd.logistics_warehouse_order_in_doc (
        -- 主键标识
        inid bigint COMMENT '入库单ID',
        dw_updatetime datetime COMMENT '数据更新时间',
        -- 时间
        create_time datetime COMMENT '建立时间',
        -- 仓库
        warehid bigint COMMENT '仓库ID',
        warehouse_name varchar COMMENT '仓库名称',
        -- 货主
        goodsownerid bigint COMMENT '货主ID',
        goodsowner_name varchar COMMENT '货主名称',
        -- 状态
        usestatus int COMMENT '使用状态',
        use_status varchar COMMENT '使用状态:0-取消/1-下单/2-处理中/3-完成/4-挂起',
        is_autotask tinyint COMMENT '是否转单自动生成订单',
        operation_type varchar COMMENT '业务类型'
    ) UNIQUE KEY (inid, dw_updatetime) DISTRIBUTED BY HASH (inid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO
    dwd.logistics_warehouse_order_in_doc (
        -- 主键标识
        inid,
        dw_updatetime,
        -- 时间
        create_time,
        -- 仓库
        warehid,
        warehouse_name,
        -- 货主
        goodsownerid,
        goodsowner_name,
        -- 状态
        usestatus,
        use_status,
        is_autotask,
        operation_type
    )
SELECT
    a.inid,
    a.dw_updatetime,
    a.credate as create_time,
    a.warehid,
    b.warehname,
    a.goodsownerid,
    c.goodsownername,
    a.usestatus,
    CASE
        WHEN a.usestatus = 0 THEN '取消'
        WHEN a.usestatus = 1 THEN '下单'
        WHEN a.usestatus = 2 THEN '处理中'
        WHEN a.usestatus = 3 THEN '完成'
        WHEN a.usestatus = 4 THEN '挂起'
        ELSE '未定义'
    END AS use_status,
    IFNULL(a.autotaskflag, 0) as is_autotask,
    s.ddlname as operation_type
FROM
    ods_wms.wms_in_order a
    LEFT JOIN ods_wms.tpl_warehouse b ON a.warehid = b.warehid
    AND b.is_active = 1
    LEFT JOIN ods_wms.tpl_goodsowner c ON a.goodsownerid = c.goodsownerid
    AND c.is_active = 1
    LEFT JOIN ods_wms.sys_ddl_dtl s ON a.operationtype = s.ddlid AND s.sysid = 389 AND s.is_active = 1
WHERE
    a.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_warehid ON dwd.logistics_warehouse_order_in_doc (warehid);
CREATE INDEX IF NOT EXISTS idx_goodsownerid ON dwd.logistics_warehouse_order_in_doc (goodsownerid);
CREATE INDEX IF NOT EXISTS idx_usestatus ON dwd.logistics_warehouse_order_in_doc (usestatus);
