DROP TABLE IF EXISTS dim.eshop_entry_goods;
CREATE TABLE dim.eshop_entry_goods (
        entryid bigint COMMENT '独立单元ID',
        goodsid bigint COMMENT '客户ID',
        dw_starttime datetime COMMENT '商品上线日期',
        dw_endtime datetime COMMENT '商品下线日期',
        org_id bigint COMMENT '独立单元id对应电商id',
        product_id bigint COMMENT '商品id对应电商id'
    ) UNIQUE KEY (entryid, goodsid, dw_starttime) DISTRIBUTED BY HASH (goodsid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );


-- 插入数据到dim.eshop_entry_goods表
INSERT INTO dim.eshop_entry_goods (
    entryid,
    goodsid,
    dw_starttime,
    dw_endtime,
    ori_id,
    product_id
)
WITH
    ranked_eshop_entry_goods AS (
        SELECT
            entryid,
            goodsid,
            tob2bdate,
            MAX(dw_createtime) as dw_createtime,
            MAX(dw_updatetime) as dw_updatetime
        FROM
            ods_erp.eshop_entry_goods
        WHERE
            entryid IS NOT NULL
            AND goodsid IS NOT NULL
            AND tob2bdate IS NOT NULL
        GROUP BY
            entryid,
            goodsid,
            tob2bdate
    )
SELECT
    ec.entryid,
    ec.goodsid,
    ec.tob2bdate as dw_starttime,
    CASE
      WHEN ec.dw_createtime <> ec.dw_updatetime THEN date(ec.dw_updatetime)
      ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
    END as dw_endtime,
    e.org_id,
    g.product_id
FROM
    ranked_eshop_entry_goods ec
LEFT JOIN
    (SELECT entryid, MIN(org_id) as org_id FROM dim.entry group by entryid) e ON ec.entryid = e.entryid
LEFT JOIN
    (SELECT PRODUCT_CODE, MIN(PRODUCT_ID) as product_id FROM ods_dsys.prd_product group by PRODUCT_CODE) g ON ec.goodsid = g.PRODUCT_CODE;


CREATE INDEX IF NOT EXISTS idx_startdates ON dim.eshop_entry_goods (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.eshop_entry_goods (dw_endtime);
