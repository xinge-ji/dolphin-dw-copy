import csv
import os

input_csv = "log/erp_function.csv"  # 你的 CSV 文件路径
output_dir = "sql/function/wms/oracle"  # 输出目录
os.makedirs(output_dir, exist_ok=True)

with open(input_csv, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["OWNER"] != "LYWMS":
            continue

        view_name = row["FUNCTION_NAME"]
        raw_sql = row["FUNCTION_DDL"]

        # 处理 Oracle CSV 中的 CLOB 转义
        # 把双双引号 ("") 还原成单个双引号 (")
        sql_text = raw_sql.replace('""', '"').strip()
        # sql_text = sqlglot.transpile(sql_text, read="oracle", write="doris")[0]

        # 写入 .sql 文件
        output_path = os.path.join(output_dir, f"{view_name}.sql")
        with open(output_path, "w", encoding="utf-8") as out_file:
            out_file.write(sql_text)

        print(f"Generated: {output_path}")
