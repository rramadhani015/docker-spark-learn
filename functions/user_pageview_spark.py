import glob
import os
import shutil


import pyspark
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.hooks.postgres_hook import PostgresHook

from helpers.helper import pg_to_df, query_file, spark_analytics
# from helpers.helper import pg_to_df, query_file

redshift = PostgresHook('redshift')
analytics = PostgresHook('analytics')

class PageViewAnalyticsS3():
    def __init__(self):
        self.s3_path = '/mnt/s3/dw-gdn/etl'
        self.task_name = 'user_pageview_spark'
        self.last_id_query = query_file(
            '/opt/airflow/dags/queries/pageview_last_id.sql')
        self.analytics_query = query_file(
            '/opt/airflow/dags/queries/pageview_analytics.sql')
        self.analytics_query_limit = query_file(
            '/opt/airflow/dags/queries/pageview_analytics_limit.sql')
        self.remove_old_files()

    def remove_old_files(self):
        # pass
        shutil.rmtree(f"{self.s3_path}/{self.task_name}/{self.task_name}.parquet")
        # list_of_files = glob.glob(f"{self.s3_path}/{self.task_name}/{self.task_name}.parquet/*")
        # for file in list_of_files:
        #     os.remove(file)

    def analytics_query_builder(self):
        return f"""
        select *
        from ({self.analytics_query}) as foo
        where id > {self.last_id}
        order by id;
        """

    def extract(self):
        last_id = redshift.get_records(self.last_id_query)
        self.last_id = last_id[0][0]

        self.df = pg_to_df(self.analytics_query_builder(), analytics)
        self.df.columns = ['id', 'created', 'datetime', 'user_id', 'session_name',
                           'ip_address', 'online_status', 'device_id', 'device_model',
                           'os_version', 'client_id', 'client_version', 'item_id', 'page_orientation',
                           'page_number', 'second', 'organization_id', 'catalog_id']

    def transform(self):
        df = self.df
        df['id'] = df['id'].astype('int64')
        df['created'] = pd.to_datetime(df['created'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['user_id'] = df['user_id'].fillna(0).astype('int64')
        df['client_id'] = df['client_id'].fillna(0).astype('int64')
        df['item_id'] = df['item_id'].fillna(0).astype('int64')
        df['second'] = pd.to_numeric(df['second'])
        df['organization_id'] = df['organization_id'].fillna(0).astype('int64')
        df['catalog_id'] = df['catalog_id'].fillna(0).astype('int64')
        self.df = df

    def load(self):
        file_name = f"{self.s3_path}/{self.task_name}/{self.task_name}.parquet"
        table = pa.Table.from_pandas(self.df)
        pq.write_table(table, file_name, coerce_timestamps='ms',
                       allow_truncated_timestamps=True, compression='snappy')

    def run_spark(self):
        file_name = f"{self.s3_path}/{self.task_name}/{self.task_name}.parquet"
        spark_df= spark_analytics(self.analytics_query_limit,'user_page_view')
        spark_df.write.parquet(file_name)
