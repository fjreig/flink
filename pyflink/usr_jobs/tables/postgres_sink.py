from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    jar_files = [
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
        "file:///opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        "file:///opt/flink/lib/postgresql-42.7.3.jar",
    ]
    jar_files_str = ";".join(jar_files)

    # Set the configuration
    env = StreamExecutionEnvironment.get_execution_environment()
    tbl_env = StreamTableEnvironment.create(env)
    tbl_env.get_config().set("pipeline.jars", jar_files_str)
    tbl_env.get_config().set("parallelism.default", "4")

    #######################################################################
    # Create Kafka Source Table with DDL
    #######################################################################
    src_ddl = """
        CREATE TABLE sales_usd (
            seller_id VARCHAR,
            amount_usd DOUBLE,
            sale_ts BIGINT,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-usd',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'properties.group.id' = 'sales-usd',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        )
    """

    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('sales_usd')

    print('\nSource Schema')
    tbl.print_schema()

    #####################################################################
    # Define Tumbling Window Aggregate Calculation (Seller Sales Per Minute)
    #####################################################################
    sql = """
        SELECT
          seller_id,
          window_start, 
          window_end,
          SUM(amount_usd) * 0.85 AS window_sales
        FROM TABLE(
            TUMBLE(TABLE sales_usd, DESCRIPTOR(proctime), INTERVAL '60' SECONDS))
        GROUP BY seller_id, window_start, window_end;
    """
    revenue_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    revenue_tbl.print_schema()

    ###############################################################
    # Create Kafka Sink Table
    ###############################################################
    sink_ddl = """
        CREATE TABLE sales_euros (
            seller_id VARCHAR,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            window_sales DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/monitorizacion',
            'table-name' = 'sales_euros',
            'username' = 'postgres',
            'password' = 'postgres'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    revenue_tbl.execute_insert('sales_euros').wait()

    tbl_env.execute('postgres-table-sink')

if __name__ == '__main__':
    main()
