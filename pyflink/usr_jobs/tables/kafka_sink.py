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
        CREATE TABLE fv_table (
            planta_id VARCHAR,
            potencia DOUBLE,
            radiacion DOUBLE,
            time_ts BIGINT,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'fv_plantas',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'properties.group.id' = 'fv_group',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        )
    """

    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('fv_table')

    print('\nSource Schema')
    tbl.print_schema()

    #####################################################################
    # Define Tumbling Window Aggregate Calculation
    #####################################################################
    sql = """
        SELECT
          planta_id, window_start, window_end,
          round(avg(potencia),2) AS Potencia, round(avg(radiacion),2) AS Radiacion
        FROM TABLE(
            TUMBLE(TABLE fv_table, DESCRIPTOR(proctime), INTERVAL '60' SECONDS))
        GROUP BY planta_id, window_start, window_end;
    """
    revenue_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    revenue_tbl.print_schema()

    ###############################################################
    # Create Kafka Sink Table
    ###############################################################
    sink_ddl = """
        CREATE TABLE fv_table2 (
            planta_id VARCHAR,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            Potencia DOUBLE,
            Radiacion DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'fv_table',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    revenue_tbl.execute_insert('fv_table2').wait()

    tbl_env.execute('kafka-table-sink')

if __name__ == '__main__':
    main()
