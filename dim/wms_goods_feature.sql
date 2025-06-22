DROP TABLE IF EXISTS dim.wms_goods_feature;

CREATE TABLE
    dim.wms_goods_feature (
        featureid bigint COMMENT '特征ID',
        dw_starttime datetime COMMENT '数据开始时间',
        dw_endtime datetime COMMENT '数据结束时间',
        is_active tinyint COMMENT '是否有效',
        warehid bigint COMMENT '仓库ID',
        goodsid bigint COMMENT '商品ID',
        goodsclass bigint COMMENT '商品类别ID',
        goods_class_name varchar COMMENT '商品类别',
        is_coldchain tinyint COMMENT '是否冷链',
        is_chinese_medicine tinyint COMMENT '是否中药',
        goods_category varchar COMMENT '商品分类:冷链/中药/其他'
    ) UNIQUE KEY (featureid, dw_starttime) DISTRIBUTED BY HASH (featureid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO
    dim.wms_goods_feature (
        featureid,
        dw_starttime,
        dw_endtime,
        is_active,
        warehid,
        goodsid,
        goodsclass,
        goods_class_name,
        is_coldchain,
        is_chinese_medicine,
        goods_category
    )
WITH
    ranked_wms_goods_feature AS (
        SELECT
            featureid,
            dw_createtime,
            dw_updatetime,
            warehid,
            goodsid,
            goodsclass,
            ROW_NUMBER() OVER (
                PARTITION BY
                    featureid
                ORDER BY
                    dw_createtime
            ) AS record_seq,
            LEAD (date (dw_createtime), 1, NULL) OVER (
                PARTITION BY
                    featureid
                ORDER BY
                    dw_createtime
            ) AS next_start_time
        FROM
            ods_wms.wms_goods_feature
    )
SELECT
    a.featureid,
    CASE
        WHEN a.record_seq = 1 THEN LEAST (date ('1970-01-01'), date (a.dw_createtime))
        ELSE date (a.dw_createtime)
    END AS dw_starttime,
    CASE
        WHEN a.next_start_time IS NOT NULL THEN a.next_start_time
        WHEN a.dw_createtime <> a.dw_updatetime THEN date (a.dw_updatetime)
        ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
    END AS dw_endtime,
    CASE
        WHEN a.next_start_time IS NOT NULL THEN 0
        WHEN a.dw_createtime <> a.dw_updatetime THEN 0
        ELSE 1
    END AS is_active,
    a.warehid,
    a.goodsid,
    a.goodsclass,
    e.ddlname AS goods_class_name,
    IFNULL(e.chainflag, 0) AS is_coldchain,
    CASE
        WHEN a.goodsclass in (14, 15, 16, 20, 47) THEN 1
        ELSE 0
    END as is_chinese_medicine,
    CASE
        WHEN IFNULL(e.chainflag, 0) = 1 THEN '冷链'
        WHEN a.goodsclass in (14, 15, 16, 20, 47) THEN '中药'
        ELSE '其他'
    END as goods_category
FROM
    ranked_wms_goods_feature a
    JOIN ods_wms.pub_ddl_dtl e ON a.goodsclass = e.ddlid AND e.sysid = 9 AND e.is_active = 1;

-- 创建索引以提升查询性能
CREATE INDEX IF NOT EXISTS idx_wms_goods_feature_warehid ON dim.wms_goods_feature (warehid);
CREATE INDEX IF NOT EXISTS idx_wms_goods_feature_goodsid ON dim.wms_goods_feature (goodsid);
CREATE INDEX IF NOT EXISTS idx_wms_goods_feature_goodsclass ON dim.wms_goods_feature (goodsclass);
