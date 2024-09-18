from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=settings)

    #######################################################################
    # Create Table with DDL from CSV
    #######################################################################
    src_ddl = """
        CREATE TABLE iot_telemetry (
            ts VARCHAR,
            device VARCHAR,
            co VARCHAR,
            humidity VARCHAR,
            light VARCHAR,
            lpg VARCHAR,
            motion VARCHAR,
            smoke VARCHAR,
            temp VARCHAR
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '/opt/flink/usr_jobs/python/sensor-source/iot_telemetry_data.csv'
        )
    """

    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('iot_telemetry')

    print('\nSource Schema')
    tbl.print_schema()

    print('\nQuery')
    tbl2 = tbl.select(tbl.ts, tbl.device, tbl.temp).where(tbl.temp >= "20")
    tbl2.execute().print()

if __name__ == '__main__':
    main()