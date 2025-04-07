DROP TABLE IF EXISTS dim.goods_variety;

CREATE TABLE
    dim.goods_variety (
        varietyid bigint COMMENT '商品小类ID',
        varietydescid bigint COMMENT '商品中类ID',
        vardesclassid bigint COMMENT '商品大类ID',
        dw_starttime datetime COMMENT '数据开始时间',
        dw_endtime datetime COMMENT '数据结束时间',
        is_active tinyint COMMENT '是否有效',
        variety_level1_name varchar COMMENT '商品小类名称',
        variety_level2_name varchar COMMENT '商品中类名称',
        variety_level3_name varchar COMMENT '商品大类名称'
    ) UNIQUE KEY (
        varietyid,
        varietydescid,
        vardesclassid,
        dw_starttime
    ) DISTRIBUTED BY HASH (varietyid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO
    dim.goods_variety (
        varietyid,
        varietydescid,
        vardesclassid,
        dw_starttime,
        dw_endtime,
        is_active,
        variety_level1_name,
        variety_level2_name,
        variety_level3_name
    )
WITH
    variety_data AS (
        SELECT
            CAST(varietyid as bigint) as varietyid,
            CAST(varietydescid as bigint) as varietydescid,
            varietyname as variety_level1_name,
            dw_createtime,
            dw_updatetime,
            ROW_NUMBER() OVER (
                PARTITION BY
                    varietyid
                ORDER BY
                    dw_createtime
            ) AS record_seq,
            LEAD (date(dw_createtime), 1, NULL) OVER (
                PARTITION BY
                    varietyid
                ORDER BY
                    dw_createtime
            ) AS next_start_time
        FROM
            ods_erp.pub_goods_variety
    ),
    variety_with_dates AS (
        SELECT
            varietyid,
            varietydescid,
            variety_level1_name,
            CASE
                WHEN record_seq = 1 THEN LEAST (date ('1970-01-01'), date(dw_createtime))
                ELSE date(dw_createtime)
            END AS dw_starttime,
            CASE
                WHEN next_start_time IS NOT NULL THEN next_start_time
                WHEN dw_createtime <> dw_updatetime THEN date(dw_updatetime)
                ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
            END AS dw_endtime
        FROM
            variety_data
    ),
    variety_desc_data AS (
        SELECT
            CAST(varietydescid as bigint) as varietydescid,
            CAST(vardesclassid as bigint) as vardesclassid,
            varietydescname as variety_level2_name,
            dw_createtime,
            dw_updatetime,
            ROW_NUMBER() OVER (
                PARTITION BY
                    varietydescid
                ORDER BY
                    dw_createtime
            ) AS record_seq,
            LEAD (date(dw_createtime), 1, NULL) OVER (
                PARTITION BY
                    varietydescid
                ORDER BY
                    dw_createtime
            ) AS next_start_time
        FROM
            ods_erp.pub_goods_variety_desc
    ),
    variety_desc_with_dates AS (
        SELECT
            varietydescid,
            vardesclassid,
            variety_level2_name,
            CASE
                WHEN record_seq = 1 THEN LEAST (date ('1970-01-01'), date(dw_createtime))
                ELSE date(dw_createtime)
            END AS dw_starttime,
            CASE
                WHEN next_start_time IS NOT NULL THEN next_start_time
                WHEN dw_createtime <> dw_updatetime THEN date(dw_updatetime)
                ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
            END AS dw_endtime
        FROM
            variety_desc_data
    ),
    variety_desc_class_data AS (
        SELECT
            CAST(vardesclassid as bigint) as vardesclassid,
            vardesclassname as variety_level3_name,
            dw_createtime,
            dw_updatetime,
            ROW_NUMBER() OVER (
                PARTITION BY
                    vardesclassid
                ORDER BY
                    dw_createtime
            ) AS record_seq,
            LEAD (date(dw_createtime), 1, NULL) OVER (
                PARTITION BY
                    vardesclassid
                ORDER BY
                    dw_createtime
            ) AS next_start_time
        FROM
            ods_erp.pub_goods_variety_desc_class
    ),
    variety_desc_class_with_dates AS (
        SELECT
            vardesclassid,
            variety_level3_name,
            CASE
                WHEN record_seq = 1 THEN LEAST (date ('1970-01-01'), date(dw_createtime))
                ELSE date(dw_createtime)
            END AS dw_starttime,
            CASE
                WHEN next_start_time IS NOT NULL THEN next_start_time
                WHEN dw_createtime <> dw_updatetime THEN date(dw_updatetime)
                ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
            END AS dw_endtime
        FROM
            variety_desc_class_data
    )
SELECT
    v.varietyid,
    v.varietydescid,
    vd.vardesclassid,
    v.dw_starttime,
    v.dw_endtime,
    CASE 
        WHEN v.dw_endtime = CAST('9999-12-31 23:59:59' AS DATETIME) THEN 1
        ELSE 0
    END AS is_active,
    v.variety_level1_name,
    vd.variety_level2_name,
    vdc.variety_level3_name
FROM
    variety_with_dates v
    LEFT JOIN variety_desc_with_dates vd ON v.varietydescid = vd.varietydescid
    AND v.dw_starttime < vd.dw_endtime
    AND v.dw_endtime >= vd.dw_starttime
    LEFT JOIN variety_desc_class_with_dates vdc ON vd.vardesclassid = vdc.vardesclassid
    AND vd.dw_starttime < vdc.dw_endtime
    AND vd.dw_endtime >= vdc.dw_starttime;


-- 先删除索引（如果存在）
DROP INDEX IF EXISTS idx_varietyid ON dim.goods_variety;
DROP INDEX IF EXISTS idx_varietydescid ON dim.goods_variety;
DROP INDEX IF EXISTS idx_vardesclassid ON dim.goods_variety;
DROP INDEX IF EXISTS idx_startdates ON dim.goods_variety;
DROP INDEX IF EXISTS idx_enddates ON dim.goods_variety;

-- 然后创建索引
CREATE INDEX idx_variety ON dim.goods_variety (variety_level1_name);
CREATE INDEX idx_varietydesc ON dim.goods_variety (variety_level2_name);
CREATE INDEX idx_vardesclassid ON dim.goods_variety (variety_level3_name);
CREATE INDEX idx_startdates ON dim.goods_variety (dw_starttime);
CREATE INDEX idx_enddates ON dim.goods_variety (dw_endtime);