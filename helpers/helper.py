import pandas as pd
from pyspark.sql import SparkSession

def query_file(path):
    with open(path) as f:
        query = f.read()
    return query


def pg_to_df(query, pg_hook):
    conn = pg_hook.get_conn()

    try:
        return pd.read_sql_query(query, conn)
    except Exception as error:
        raise("Error fetching data from PostgreSQL:", error)
    finally:
        if conn:
            conn.close()


def spark_analytics(query, table_name):
    # the Spark session should be instantiated as follows
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL load analytics") \
        .config("spark.jars", "/opt/airflow/dags/postgresql-42.4.0.jar") \
        .getOrCreate()
    
    spark_df = spark.read.format("jdbc").\
    options(url='jdbc:postgresql://analytics-prod.ciupuzgrmaar.ap-southeast-1.rds.amazonaws.com/analyticsprod', 
            dbtable=table_name,
            user='analyticsprod',
            password='analyticsprod',
            driver='org.postgresql.Driver').load()   

    spark_df.createOrReplaceTempView(table_name)
    spark_df = spark.sql(query)  
    return spark_df
    # try:
    #     # jdbcDF.show()
    #     # Register the DataFrame as a SQL temporary view
    #     spark_df.createOrReplaceTempView(table_name)
    #     return spark_df = spark.sql(query)
    # except Exception as error:
    #     raise("Error fetching data from PostgreSQL:", error)
