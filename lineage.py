from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
    AuthProvider,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.ingestion.lineage.sql_lineage import get_lineage_by_query
from metadata.ingestion.lineage.models import Dialect
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from sqlalchemy import create_engine, text
import pandas as pd
import glob

server_config = OpenMetadataConnection(
    hostPort="http://10.10.30.130:8585/api",
    authProvider=AuthProvider.openmetadata,
    securityConfig=OpenMetadataJWTClientConfig(
        jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NDUzMDQxNTksImV4cCI6bnVsbH0.w5G7_WjpvHD4XJUcR30CtEM9GxffypxibEYuOsIwF2IG4gvEta9wAHuQwlu_YFDQ5woJFRRnRVZWXhYLBO8CjIT3JsXdGIXKHfKZVpE9RrLh1A5_5qIn6rhte8H3VltPtiUjgVsWGEwSoHUCSuPivqAnOKwBS1NVy4Pw54tpNDgmAApNF9gH6LXKcjDrNOM8gwvXDQeu0JUgCYz0wutK9Y6KEovlcrF-Ic2MwciyUkcprm0Vdw2aHwtg1SubsqTJB1kWa4aWIkbsg3SshId7dKDy0ZRilez7VptgSZ91_32wbhmOtmub7sbRR1w75Mr5yYERRKL-lqkMWnM2k56lRA",
    ),
)

metadata = OpenMetadata(server_config)


def get_oracle_view_query(db):
    if db == "ERP":
        username = "lyerp"
    elif db == "WMS":
        username = "lywms"
    address = f"oracle+cx_oracle://{username}:{username}@10.10.10.202:1521/?service_name=luyandg"

    try:
        engine = create_engine(address)

        query = """
        SELECT view_name, 
               dbms_metadata.get_ddl('VIEW', view_name, owner) as view_text
        FROM all_views 
        WHERE owner = :owner
        """

        with engine.connect() as connection:
            result = connection.execute(text(query), {"owner": username.upper()})

            rows = []
            columns = result.keys()

            for row in result:
                rows.append(dict(zip(columns, row)))

            df_view = pd.DataFrame(rows)

            return df_view
    except Exception as e:
        print(f"Error connecting to {db} database: {str(e)}")
        return pd.DataFrame()


def insert_view_lineage(db):
    database_service: DatabaseService = metadata.get_by_name(
        entity=DatabaseService, fqn=db
    )
    if db == "ERP":
        username = "lyerp"
    elif db == "WMS":
        username = "lywms"
    
    df_view = get_oracle_view_query(db)
    for _, row in df_view.iterrows():
        view_text = row["view_text"]
        metadata.add_lineage_by_query(
            database_service, view_text, username, username, 60
        )

def insert_doris_lineage():
    sql_files = glob.glob("workflow/**/*_create.sql", recursive=True)
    database_service: DatabaseService = metadata.get_by_name(
        entity=DatabaseService, fqn='Doris'
    )
    for sql_file in sql_files:
        try:
            # 读取SQL文件内容
            with open(sql_file, "r", encoding="utf-8") as f:
                query = f.read()

            # 添加血缘关系
            metadata.add_lineage_by_query(database_service, query, timeout=60)

            print(f"成功为 {sql_file} 添加血缘关系")
        except Exception as e:
            print(f"处理文件 {sql_file} 时出错: {str(e)}")
