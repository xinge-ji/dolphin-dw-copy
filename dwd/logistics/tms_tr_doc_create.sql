DROP TABLE IF EXISTS dwd.logistics_tms_tr_doc;

CREATE TABLE 
    dwd.logistics_tms_tr_doc (
        -- 主键标识
        trid bigint COMMENT '运输单ID',

        -- 时间
        dw_updatetime datetime COMMENT '数据更新时间',
        sign_time datetime COMMENT '签收时间',

        -- 仓库
        warehid bigint COMMENT '仓库ID',

        -- 货主
        goodsownerid bigint COMMENT '货主ID',
        goodsowner_name varchar COMMENT '货主名称',

        -- 客户
        company_name varchar COMMENT '客户名称',

        -- 调度单
        dispatchid bigint COMMENT '调度单ID',

        -- 随货同行单
        goodspeerno bigint COMMENT '随货同行单号'
    ) UNIQUE KEY (trid) DISTRIBUTED BY HASH (trid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO dwd.logistics_tms_tr_doc (
    trid,
    dw_updatetime,
    sign_time,
    warehid,
    goodsownerid,
    goodsowner_name,
    company_name,
    dispatchid,
    goodspeerno
)
SELECT
    a.trid,
    a.dw_updatetime,
    a.signdate AS sign_time,
    a.warehid,
    a.goodsownerid,
    b.goodsownername AS goodsowner_name,
    c.companyname AS company_name,
    a.dispatchid,
    a.sourceid as goodspeerno
FROM
    ods_wms.TMS_TR_DOC a
JOIN ods_wms.tpl_goodsowner b ON a.goodsownerid = b.goodsownerid AND b.is_active = 1
JOIN ods_wms.tpl_go_company c ON a.gcompanyid = c.companyid AND c.is_active = 1
WHERE a.is_active = 1;