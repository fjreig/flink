from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=settings)

    # add kafka connector dependency
    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")

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
          TUMBLE_END(proctime, INTERVAL '60' SECONDS) AS window_end,
          SUM(amount_usd) * 0.85 AS window_sales
        FROM sales_usd
        GROUP BY
          TUMBLE(proctime, INTERVAL '60' SECONDS),
          seller_id
    """

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
            'connector' = 'kafka',
            'topic' = 'sales-euros',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    revenue_tbl.execute_insert('sales_euros').wait()

    tbl_env.execute('windowed-sales-euros')

if __name__ == '__main__':
    main()