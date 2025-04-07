DROP TABLE IF EXISTS dwd.wholesale_jicai_volume_doc;
CREATE TABLE dwd.wholesale_jicai_volume_doc (
	docid bigint COMMENT '集采带量ID',
    goodsid bigint COMMENT '商品ID',
    entryid bigint COMMENT '独立单元ID',
    dw_updatetime datetime COMMENT '数据更新时间',
    dlbegindate datetime COMMENT '带量开始日期',
    dlenddate bigint COMMENT '带量结束日期'
)
UNIQUE KEY(docid) DISTRIBUTED BY HASH(docid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

INSERT INTO dwd.wholesale_jicai_volume_doc (
    docid,
    goodsid,
    entryid,
    dw_updatetime,
    dlbegindate,
    dlenddate
)
SELECT
	docid,
    goodsid,
    entryid,
    dw_updatetime,
    dlbegindate,
    dlenddate
FROM
	ods_erp.T_101248_DOC
WHERE is_active = 1;

CREATE INDEX IF NOT EXISTS idx_goodsid ON dwd.wholesale_jicai_volume_doc (goodsid);
CREATE INDEX IF NOT EXISTS idx_entryid ON dwd.wholesale_jicai_volume_doc (entryid);
CREATE INDEX IF NOT EXISTS idx_dlbegindate ON dwd.wholesale_jicai_volume_doc (dlbegindate);
CREATE INDEX IF NOT EXISTS idx_dlenddate ON dwd.wholesale_jicai_volume_doc (dlenddate);
