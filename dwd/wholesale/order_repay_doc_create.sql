DROP TABLE IF EXISTS dwd.wholesale_order_repay_doc;
CREATE TABLE dwd.wholesale_order_repay_doc (
    -- 颗粒度
    sarecid bigint COMMENT '还款总单ID',

    -- 基础信息
    dw_updatetime datetime COMMENT '更新时间',

    -- 收款单信息
    create_date datetime COMMENT '创建日期',
    confirm_date datetime COMMENT '确认日期',
    shoukuan_type varchar COMMENT '收款类型：预收款/现收/收欠款',
    use_status varchar COMMENT '使用状态：作废/正式',
    is_yibao_payment tinyint COMMENT '是否医保支付'
)
UNIQUE KEY(sarecid) DISTRIBUTED BY HASH(sarecid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

INSERT INTO dwd.wholesale_order_repay_doc (
    sarecid,
    dw_updatetime,
    create_date,
    confirm_date,
    shoukuan_type,
    use_status,
    is_yibao_payment
)
SELECT
    sarecid,
    dw_updatetime,
    credate,
    confirmdate,
    CASE 
    	WHEN recmethod = 1 THEN '预收款'
        WHEN recmethod = 2 THEN '现收'
        WHEN recmethod = 3 THEN '收欠款'
        ELSE ''
    END as shoukuan_type,
    CASE
    	WHEN usestatus = 0 THEN '作废'
        WHEN usestatus = 1 THEN '正式'
        WHEN usestatus = 2 THEN '作废'
        ELSE ''
    END AS use_status,
    CASE
    	WHEN zx_ybflag = 1 THEN 1
        ELSE 0
    END AS is_yibao_payment
FROM ods_erp.bms_sa_rec_doc
WHERE is_active = 1;

CREATE INDEX IF NOT EXISTS idx_create_date ON dwd.wholesale_order_repay_doc (create_date);
CREATE INDEX IF NOT EXISTS idx_confirm_date ON dwd.wholesale_order_repay_doc (confirm_date);
CREATE INDEX IF NOT EXISTS idx_shoukuan_type ON dwd.wholesale_order_repay_doc (shoukuan_type);
CREATE INDEX IF NOT EXISTS idx_use_status ON dwd.wholesale_order_repay_doc (use_status);
CREATE INDEX IF NOT EXISTS idx_is_yibao_payment ON dwd.wholesale_order_repay_doc (is_yibao_payment);