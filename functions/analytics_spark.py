from airflow.models import Variable

from helpers.helper import query_file
# from functions.user_appopen import AppOpenAnalyticsS3
from functions.user_pageview_spark import PageViewAnalyticsS3


# def run_appopen_analytics_s3():
#     etl = AppOpenAnalyticsS3()
#     etl.extract()
#     etl.transform()
#     etl.load()


# def run_appopen_s3_redshift():
#     table = 'eperpus_new.user_appopen'
#     s3_filename = 'etl/user_appopen/user_appopen.parquet'
#     aws_access_key_id = Variable.get('aws_access_key_id')
#     aws_secret_access_key = Variable.get('aws_secret_access_key')
#     copy_query = query_file('/opt/airflow/dags/queries/copy_s3_redshift.sql')
#     return copy_query.format(
#         table=table,
#         s3_filename=s3_filename,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key)


def run_pageview_analytics_s3():
    etl = PageViewAnalyticsS3()
    # etl.extract()
    # etl.transform()
    # etl.load()
    etl.run_spark()


# def run_pageview_s3_redshift():
#     table = 'eperpus_new.user_pageview'
#     s3_filename = 'etl/user_pageview/user_pageview.parquet'
#     aws_access_key_id = Variable.get('aws_access_key_id')
#     aws_secret_access_key = Variable.get('aws_secret_access_key')
#     copy_query = query_file('/opt/airflow/dags/queries/copy_s3_redshift.sql')
#     return copy_query.format(
#         table=table,
#         s3_filename=s3_filename,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key)
