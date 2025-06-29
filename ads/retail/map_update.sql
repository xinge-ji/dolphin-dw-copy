DROP TABLE IF EXISTS ads.retail_map;

CREATE TABLE ads.retail_map (
    province VARCHAR(100) COMMENT '省份',
    city VARCHAR(100) COMMENT '城市',
    lat DECIMAL(10,6) COMMENT '纬度',
    lng DECIMAL(10,6) COMMENT '经度', 
    medinsName VARCHAR COMMENT '机构名称',
    addr VARCHAR COMMENT '地址',
    medinsType VARCHAR(100) COMMENT '机构类型',
    medinsTypeOrder INT COMMENT '机构类型排序'
) 
UNIQUE KEY (province, city, lat, lng, medinsName) DISTRIBUTED BY HASH (province, city) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
);

INSERT INTO ads.retail_map (
    province,
    city,
    lat,
    lng,
    addr,
    medinsName,
    medinsType,
    medinsTypeOrder
)
SELECT 
    province,
    city,
    lat,
    lnt AS lng, 
    addr,
    medinsName,
    CASE
        WHEN medinsName is not NULL AND medinsName LIKE '%鹭燕%' THEN '鹭燕'
        ELSE '零售药店'
    END AS medinsType,
    CASE
        WHEN medinsName LIKE '%鹭燕%' THEN 1
        ELSE 2
    END AS medinsTypeOrder
FROM ods_yjy.nhsa_lingshouyaodian
WHERE useStatus = '1'

UNION ALL

SELECT
    province,
    city, 
    lat,
    lnt AS lng,
    addr,
    medinsName,
    CASE
        WHEN medinsTypeName is NULL OR medinsTypeName not in ('普通诊所', '中医诊所', '综合医院', '中心卫生院', '社区卫生服务站', '中西医结合诊所', '社区卫生服务中心', '综合门诊部', '妇幼保健院', '中医（综合）医院') THEN '其他'
        ELSE medinsTypeName
    END AS medinsTypeName,
    CASE
        WHEN medinsTypeName = '普通诊所' THEN 3
        WHEN medinsTypeName = '中医诊所' THEN 4
        WHEN medinsTypeName = '综合医院' THEN 5
        WHEN medinsTypeName = '中心卫生院' THEN 6
        WHEN medinsTypeName = '社区卫生服务站' THEN 7
        WHEN medinsTypeName = '中西医结合诊所' THEN 8
        WHEN medinsTypeName = '社区卫生服务中心' THEN 9
        WHEN medinsTypeName = '综合门诊部' THEN 10
        WHEN medinsTypeName = '妇幼保健院' THEN 11
        WHEN medinsTypeName = '中医（综合）医院' THEN 12
        ELSE 13
    END AS medinsTypeOrder
FROM ods_yjy.nhsa_yiliaojigou
WHERE useStatus = '1';