DROP TABLE IF EXISTS ads.logistics_warehouse_in_category_m;

CREATE TABLE
    ads.logistics_warehouse_in_category_m (
        -- 颗粒度
        stat_yearmonth date COMMENT '统计年月',
        warehid bigint COMMENT '仓库ID',
        category varchar COMMENT '类别:所有/冷链/中药/其他',
        -- 描述
        warehouse_name varchar COMMENT '仓库名称',
        -- 指标
        mean_time_receive_to_check decimal(10, 1) COMMENT '收货到验收时间平均值',
        mean_time_check_to_flat decimal(10, 1) COMMENT '验收到平库上架时间平均值',
        mean_time_check_to_auto decimal(10, 1) COMMENT '验收到立库上架时间平均值',
        mean_time_receive_to_flat decimal(10, 1) COMMENT '收货到平库上架时间平均值',
        mean_time_receive_to_auto decimal(10, 1) COMMENT '收货到立库上架时间平均值',
        mean_working_time_receive_to_check decimal(10, 1) COMMENT '收货到验收工作时间平均值',
        mean_working_time_check_to_flat decimal(10, 1) COMMENT '验收到平库上架工作时间平均值',
        mean_working_time_check_to_auto decimal(10, 1) COMMENT '验收到立库上架工作时间平均值',
        mean_working_time_receive_to_flat decimal(10, 1) COMMENT '收货到平库上架工作时间平均值',
        mean_working_time_receive_to_auto decimal(10, 1) COMMENT '收货到立库上架工作时间平均值',
        median_time_receive_to_check decimal(10, 1) COMMENT '收货到验收时间中位数',
        median_time_check_to_flat decimal(10, 1) COMMENT '验收到平库上架时间中位数',
        median_time_check_to_auto decimal(10, 1) COMMENT '验收到立库上架时间中位数',
        median_time_receive_to_flat decimal(10, 1) COMMENT '收货到平库上架时间中位数',
        median_time_receive_to_auto decimal(10, 1) COMMENT '收货到立库上架时间中位数',
        median_working_time_receive_to_check decimal(10, 1) COMMENT '收货到验收工作时间中位数',
        median_working_time_check_to_flat decimal(10, 1) COMMENT '验收到平库上架工作时间中位数',
        median_working_time_check_to_auto decimal(10, 1) COMMENT '验收到立库上架工作时间中位数',
        median_working_time_receive_to_flat decimal(10, 1) COMMENT '收货到平库上架工作时间中位数',
        median_working_time_receive_to_auto decimal(10, 1) COMMENT '收货到立库上架工作时间中位数',
        p95_time_receive_to_check decimal(10, 1) COMMENT '收货到验收时间95分位数',
        p95_time_check_to_flat decimal(10, 1) COMMENT '验收到平库上架时间95分位数',
        p95_time_check_to_auto decimal(10, 1) COMMENT '验收到立库上架时间95分位数',
        p95_time_receive_to_flat decimal(10, 1) COMMENT '收货到平库上架时间95分位数',
        p95_time_receive_to_auto decimal(10, 1) COMMENT '收货到立库上架时间95分位数',
        p95_working_time_receive_to_check decimal(10, 1) COMMENT '收货到验收工作时间95分位数',
        p95_working_time_check_to_flat decimal(10, 1) COMMENT '验收到平库上架工作时间95分位数',
        p95_working_time_check_to_auto decimal(10, 1) COMMENT '验收到立库上架工作时间95分位数',
        p95_working_time_receive_to_flat decimal(10, 1) COMMENT '收货到平库上架工作时间95分位数',
        p95_working_time_receive_to_auto decimal(10, 1) COMMENT '收货到立库上架工作时间95分位数',
        h24_finish_rate_receive_to_check decimal(10, 1) COMMENT '收货到验收24小时完成率',
        h24_finish_rate_check_to_flat decimal(10, 1) COMMENT '验收到平库上架24小时完成率',
        h24_finish_rate_check_to_auto decimal(10, 1) COMMENT '验收到立库上架24小时完成率',
        h24_finish_rate_receive_to_flat decimal(10, 1) COMMENT '收货到平库上架24小时完成率',
        h24_finish_rate_receive_to_auto decimal(10, 1) COMMENT '收货到立库上架24小时完成率',
        h48_finish_rate_receive_to_flat decimal(10, 1) COMMENT '收货到平库上架48小时完成率',
        h48_finish_rate_receive_to_auto decimal(10, 1) COMMENT '收货到立库上架48小时完成率'
    ) UNIQUE KEY (
        stat_yearmonth,
        warehid,
        category
    ) DISTRIBUTED BY HASH (
        stat_yearmonth,
        warehid,
        category
    ) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );
    
INSERT INTO
    ads.logistics_warehouse_in_category_m (
        stat_yearmonth,
        warehid,
        category,
        warehouse_name,
        mean_time_receive_to_check,
        mean_time_check_to_flat,
        mean_time_check_to_auto,
        mean_time_receive_to_flat,
        mean_time_receive_to_auto,
        mean_working_time_receive_to_check,
        mean_working_time_check_to_flat,
        mean_working_time_check_to_auto,
        mean_working_time_receive_to_flat,
        mean_working_time_receive_to_auto,
        median_time_receive_to_check,
        median_time_check_to_flat,
        median_time_check_to_auto,
        median_time_receive_to_flat,
        median_time_receive_to_auto,
        median_working_time_receive_to_check,
        median_working_time_check_to_flat,
        median_working_time_check_to_auto,
        median_working_time_receive_to_flat,
        median_working_time_receive_to_auto,
        p95_time_receive_to_check,
        p95_time_check_to_flat,
        p95_time_check_to_auto,
        p95_time_receive_to_flat,
        p95_time_receive_to_auto,
        p95_working_time_receive_to_check,
        p95_working_time_check_to_flat,
        p95_working_time_check_to_auto,
        p95_working_time_receive_to_flat,
        p95_working_time_receive_to_auto,
        h24_finish_rate_receive_to_check,
        h24_finish_rate_check_to_flat,
        h24_finish_rate_check_to_auto,
        h24_finish_rate_receive_to_flat,
        h24_finish_rate_receive_to_auto,
        h48_finish_rate_receive_to_flat,
        h48_finish_rate_receive_to_auto
    )
WITH
    -- 基础数据，同时包含具体类别和"所有"类别
    base_data_receive_check AS (
        -- 具体类别数据
        SELECT DISTINCT
            warehid,
            warehouse_name,
            goods_category as category,
            DATE_TRUNC(check_time, 'MONTH') as stat_date,
            inid,
            indtlid,
            receiveid,
            time_receive_to_check / 60.0 as time_receive_to_check_hours,
            working_time_receive_to_check / 60.0 as working_time_receive_to_check_hours,
            CASE WHEN time_receive_to_check <= 1440 THEN 1 ELSE 0 END as h24_receive_to_check
        FROM dwd.logistics_warehouse_in_time
        WHERE is_autotask = 0 
            AND receiveid IS NOT NULL 
            AND time_receive_to_check IS NOT NULL
        
        UNION ALL
        
        -- "所有"类别数据
        SELECT DISTINCT
            warehid,
            warehouse_name,
            '所有' as category,
            DATE_TRUNC(check_time, 'MONTH') as stat_date,
            inid,
            indtlid,
            receiveid,
            time_receive_to_check / 60.0 as time_receive_to_check_hours,
            working_time_receive_to_check / 60.0 as working_time_receive_to_check_hours,
            CASE WHEN time_receive_to_check <= 1440 THEN 1 ELSE 0 END as h24_receive_to_check
        FROM dwd.logistics_warehouse_in_time
        WHERE is_autotask = 0 
            AND receiveid IS NOT NULL 
            AND time_receive_to_check IS NOT NULL
    ),
    base_data_check_flat AS (
        -- 具体类别数据
        SELECT DISTINCT
            warehid,
            warehouse_name,
            goods_category as category,
            DATE_TRUNC(finish_time, 'MONTH') as stat_date,
            inid,
            indtlid,
            receiveid,
            inoutid,
            time_check_to_flat / 60.0 as time_check_to_flat_hours,
            working_time_check_to_flat / 60.0 as working_time_check_to_flat_hours,
            CASE WHEN time_check_to_flat <= 1440 THEN 1 ELSE 0 END as h24_check_to_flat
        FROM dwd.logistics_warehouse_in_time
        WHERE is_autotask = 0 
            AND inoutid IS NOT NULL 
            AND time_check_to_flat IS NOT NULL
        
        UNION ALL
        
        -- "所有"类别数据
        SELECT DISTINCT
            warehid,
            warehouse_name,
            '所有' as category,
            DATE_TRUNC(finish_time, 'MONTH') as stat_date,
            inid,
            indtlid,
            receiveid,
            inoutid,
            time_check_to_flat / 60.0 as time_check_to_flat_hours,
            working_time_check_to_flat / 60.0 as working_time_check_to_flat_hours,
            CASE WHEN time_check_to_flat <= 1440 THEN 1 ELSE 0 END as h24_check_to_flat
        FROM dwd.logistics_warehouse_in_time
        WHERE is_autotask = 0 
            AND inoutid IS NOT NULL 
            AND time_check_to_flat IS NOT NULL
    ),
    base_data_check_auto AS (
        -- 具体类别数据
        SELECT DISTINCT
            warehid,
            warehouse_name,
            goods_category as category,
            DATE_TRUNC(iwcs_finish_time, 'MONTH') as stat_date,
            inid,
            indtlid,
            receiveid,
            ssc_receive_goods_locate_id,
            time_check_to_auto / 60.0 as time_check_to_auto_hours,
            working_time_check_to_auto / 60.0 as working_time_check_to_auto_hours,
            CASE WHEN time_check_to_auto <= 1440 THEN 1 ELSE 0 END as h24_check_to_auto
        FROM dwd.logistics_warehouse_in_time
        WHERE is_autotask = 0 
            AND ssc_receive_goods_locate_id IS NOT NULL 
            AND time_check_to_auto IS NOT NULL
        
        UNION ALL
        
        -- "所有"类别数据
        SELECT DISTINCT
            warehid,
            warehouse_name,
            '所有' as category,
            DATE_TRUNC(iwcs_finish_time, 'MONTH') as stat_date,
            inid,
            indtlid,
            receiveid,
            ssc_receive_goods_locate_id,
            time_check_to_auto / 60.0 as time_check_to_auto_hours,
            working_time_check_to_auto / 60.0 as working_time_check_to_auto_hours,
            CASE WHEN time_check_to_auto <= 1440 THEN 1 ELSE 0 END as h24_check_to_auto
        FROM dwd.logistics_warehouse_in_time
        WHERE is_autotask = 0 
            AND ssc_receive_goods_locate_id IS NOT NULL 
            AND time_check_to_auto IS NOT NULL
    ),
    base_data_receive_flat AS (
        -- 具体类别数据
        SELECT DISTINCT
            warehid,
            warehouse_name,
            goods_category as category,
            DATE_TRUNC(finish_time, 'MONTH') as stat_date,
            inid,
            indtlid,
            receiveid,
            inoutid,
            time_receive_to_flat / 60.0 as time_receive_to_flat_hours,
            working_time_receive_to_flat / 60.0 as working_time_receive_to_flat_hours,
            CASE WHEN time_receive_to_flat <= 1440 THEN 1 ELSE 0 END as h24_receive_to_flat,
            CASE WHEN time_receive_to_flat <= 2880 THEN 1 ELSE 0 END as h48_receive_to_flat
        FROM dwd.logistics_warehouse_in_time
        WHERE is_autotask = 0 
            AND inoutid IS NOT NULL 
            AND time_receive_to_flat IS NOT NULL
        
        UNION ALL
        
        -- "所有"类别数据
        SELECT DISTINCT
            warehid,
            warehouse_name,
            '所有' as category,
            DATE_TRUNC(finish_time, 'MONTH') as stat_date,
            inid,
            indtlid,
            receiveid,
            inoutid,
            time_receive_to_flat / 60.0 as time_receive_to_flat_hours,
            working_time_receive_to_flat / 60.0 as working_time_receive_to_flat_hours,
            CASE WHEN time_receive_to_flat <= 1440 THEN 1 ELSE 0 END as h24_receive_to_flat,
            CASE WHEN time_receive_to_flat <= 2880 THEN 1 ELSE 0 END as h48_receive_to_flat
        FROM dwd.logistics_warehouse_in_time
        WHERE is_autotask = 0 
            AND inoutid IS NOT NULL 
            AND time_receive_to_flat IS NOT NULL
    ),
    base_data_receive_auto AS (
        -- 具体类别数据
        SELECT DISTINCT
            warehid,
            warehouse_name,
            goods_category as category,
            DATE_TRUNC(iwcs_finish_time, 'MONTH') as stat_date,
            inid,
            indtlid,
            receiveid,
            ssc_receive_goods_locate_id,
            time_receive_to_auto / 60.0 as time_receive_to_auto_hours,
            working_time_receive_to_auto / 60.0 as working_time_receive_to_auto_hours,
            CASE WHEN time_receive_to_auto <= 1440 THEN 1 ELSE 0 END as h24_receive_to_auto,
            CASE WHEN time_receive_to_auto <= 2880 THEN 1 ELSE 0 END as h48_receive_to_auto
        FROM dwd.logistics_warehouse_in_time
        WHERE is_autotask = 0 
            AND ssc_receive_goods_locate_id IS NOT NULL 
            AND time_receive_to_auto IS NOT NULL
        
        UNION ALL
        
        -- "所有"类别数据
        SELECT DISTINCT
            warehid,
            warehouse_name,
            '所有' as category,
            DATE_TRUNC(iwcs_finish_time, 'MONTH') as stat_date,
            inid,
            indtlid,
            receiveid,
            ssc_receive_goods_locate_id,
            time_receive_to_auto / 60.0 as time_receive_to_auto_hours,
            working_time_receive_to_auto / 60.0 as working_time_receive_to_auto_hours,
            CASE WHEN time_receive_to_auto <= 1440 THEN 1 ELSE 0 END as h24_receive_to_auto,
            CASE WHEN time_receive_to_auto <= 2880 THEN 1 ELSE 0 END as h48_receive_to_auto
        FROM dwd.logistics_warehouse_in_time
        WHERE is_autotask = 0 
            AND ssc_receive_goods_locate_id IS NOT NULL 
            AND time_receive_to_auto IS NOT NULL
    ),
    -- 按不同时间节点分别聚合（现在包含所有类别）
    agg_receive_check AS (
        SELECT
            stat_date,
            warehid,
            category,
            warehouse_name,
            AVG(time_receive_to_check_hours) as mean_time_receive_to_check,
            AVG(working_time_receive_to_check_hours) as mean_working_time_receive_to_check,
            PERCENTILE(time_receive_to_check_hours, 0.5) as median_time_receive_to_check,
            PERCENTILE(working_time_receive_to_check_hours, 0.5) as median_working_time_receive_to_check,
            PERCENTILE(time_receive_to_check_hours, 0.95) as p95_time_receive_to_check,
            PERCENTILE(working_time_receive_to_check_hours, 0.95) as p95_working_time_receive_to_check,
            AVG(h24_receive_to_check) as h24_finish_rate_receive_to_check
        FROM base_data_receive_check
        GROUP BY stat_date, warehid, category, warehouse_name
    ),
    agg_check_flat AS (
        SELECT
            stat_date,
            warehid,
            category,
            warehouse_name,
            AVG(time_check_to_flat_hours) as mean_time_check_to_flat,
            AVG(working_time_check_to_flat_hours) as mean_working_time_check_to_flat,
            PERCENTILE(time_check_to_flat_hours, 0.5) as median_time_check_to_flat,
            PERCENTILE(working_time_check_to_flat_hours, 0.5) as median_working_time_check_to_flat,
            PERCENTILE(time_check_to_flat_hours, 0.95) as p95_time_check_to_flat,
            PERCENTILE(working_time_check_to_flat_hours, 0.95) as p95_working_time_check_to_flat,
            AVG(h24_check_to_flat) as h24_finish_rate_check_to_flat
        FROM base_data_check_flat
        GROUP BY stat_date, warehid, category, warehouse_name
    ),
    agg_check_auto AS (
        SELECT
            stat_date,
            warehid,
            category,
            warehouse_name,
            AVG(time_check_to_auto_hours) as mean_time_check_to_auto,
            AVG(working_time_check_to_auto_hours) as mean_working_time_check_to_auto,
            PERCENTILE(time_check_to_auto_hours, 0.5) as median_time_check_to_auto,
            PERCENTILE(working_time_check_to_auto_hours, 0.5) as median_working_time_check_to_auto,
            PERCENTILE(time_check_to_auto_hours, 0.95) as p95_time_check_to_auto,
            PERCENTILE(working_time_check_to_auto_hours, 0.95) as p95_working_time_check_to_auto,
            AVG(h24_check_to_auto) as h24_finish_rate_check_to_auto
        FROM base_data_check_auto
        GROUP BY stat_date, warehid, category, warehouse_name
    ),
    agg_receive_flat AS (
        SELECT
            stat_date,
            warehid,
            category,
            warehouse_name,
            AVG(time_receive_to_flat_hours) as mean_time_receive_to_flat,
            AVG(working_time_receive_to_flat_hours) as mean_working_time_receive_to_flat,
            PERCENTILE(time_receive_to_flat_hours, 0.5) as median_time_receive_to_flat,
            PERCENTILE(working_time_receive_to_flat_hours, 0.5) as median_working_time_receive_to_flat,
            PERCENTILE(time_receive_to_flat_hours, 0.95) as p95_time_receive_to_flat,
            PERCENTILE(working_time_receive_to_flat_hours, 0.95) as p95_working_time_receive_to_flat,
            AVG(h24_receive_to_flat) as h24_finish_rate_receive_to_flat,
            AVG(h48_receive_to_flat) as h48_finish_rate_receive_to_flat
        FROM base_data_receive_flat
        GROUP BY stat_date, warehid, category, warehouse_name
    ),
    agg_receive_auto AS (
        SELECT
            stat_date,
            warehid,
            category,
            warehouse_name,
            AVG(time_receive_to_auto_hours) as mean_time_receive_to_auto,
            AVG(working_time_receive_to_auto_hours) as mean_working_time_receive_to_auto,
            PERCENTILE(time_receive_to_auto_hours, 0.5) as median_time_receive_to_auto,
            PERCENTILE(working_time_receive_to_auto_hours, 0.5) as median_working_time_receive_to_auto,
            PERCENTILE(time_receive_to_auto_hours, 0.95) as p95_time_receive_to_auto,
            PERCENTILE(working_time_receive_to_auto_hours, 0.95) as p95_working_time_receive_to_auto,
            AVG(h24_receive_to_auto) as h24_finish_rate_receive_to_auto,
            AVG(h48_receive_to_auto) as h48_finish_rate_receive_to_auto
        FROM base_data_receive_auto
        GROUP BY stat_date, warehid, category, warehouse_name
    ),
    -- 获取所有维度组合
    all_dimensions AS (
        SELECT DISTINCT stat_date, warehid, category, warehouse_name
        FROM (
            SELECT stat_date, warehid, category, warehouse_name FROM agg_receive_check
            UNION
            SELECT stat_date, warehid, category, warehouse_name FROM agg_check_flat
            UNION
            SELECT stat_date, warehid, category, warehouse_name FROM agg_check_auto
            UNION
            SELECT stat_date, warehid, category, warehouse_name FROM agg_receive_flat
            UNION
            SELECT stat_date, warehid, category, warehouse_name FROM agg_receive_auto
        ) t
    )
-- 最终结果（现在包含所有类别，无需额外的UNION ALL）
SELECT
    ad.stat_date as stat_yearmonth,
    ad.warehid,
    ad.category,
    ad.warehouse_name,
    arc.mean_time_receive_to_check,
    acf.mean_time_check_to_flat,
    aca.mean_time_check_to_auto,
    arf.mean_time_receive_to_flat,
    ara.mean_time_receive_to_auto,
    arc.mean_working_time_receive_to_check,
    acf.mean_working_time_check_to_flat,
    aca.mean_working_time_check_to_auto,
    arf.mean_working_time_receive_to_flat,
    ara.mean_working_time_receive_to_auto,
    arc.median_time_receive_to_check,
    acf.median_time_check_to_flat,
    aca.median_time_check_to_auto,
    arf.median_time_receive_to_flat,
    ara.median_time_receive_to_auto,
    arc.median_working_time_receive_to_check,
    acf.median_working_time_check_to_flat,
    aca.median_working_time_check_to_auto,
    arf.median_working_time_receive_to_flat,
    ara.median_working_time_receive_to_auto,
    arc.p95_time_receive_to_check,
    acf.p95_time_check_to_flat,
    aca.p95_time_check_to_auto,
    arf.p95_time_receive_to_flat,
    ara.p95_time_receive_to_auto,
    arc.p95_working_time_receive_to_check,
    acf.p95_working_time_check_to_flat,
    aca.p95_working_time_check_to_auto,
    arf.p95_working_time_receive_to_flat,
    ara.p95_working_time_receive_to_auto,
    arc.h24_finish_rate_receive_to_check,
    acf.h24_finish_rate_check_to_flat,
    aca.h24_finish_rate_check_to_auto,
    arf.h24_finish_rate_receive_to_flat,
    ara.h24_finish_rate_receive_to_auto,
    arf.h48_finish_rate_receive_to_flat,
    ara.h48_finish_rate_receive_to_auto
FROM all_dimensions ad
LEFT JOIN agg_receive_check arc ON ad.stat_date = arc.stat_date AND ad.warehid = arc.warehid AND ad.category = arc.category
LEFT JOIN agg_check_flat acf ON ad.stat_date = acf.stat_date AND ad.warehid = acf.warehid AND ad.category = acf.category
LEFT JOIN agg_check_auto aca ON ad.stat_date = aca.stat_date AND ad.warehid = aca.warehid AND ad.category = aca.category
LEFT JOIN agg_receive_flat arf ON ad.stat_date = arf.stat_date AND ad.warehid = arf.warehid AND ad.category = arf.category
LEFT JOIN agg_receive_auto ara ON ad.stat_date = ara.stat_date AND ad.warehid = ara.warehid AND ad.category = ara.category;