from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    EnvironmentSettings,
    DataTypes,
    TableEnvironment,
    StreamTableEnvironment,
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration


def create_taxi_trip_sink(t_env):
    table_name = "processed_taxi_trips"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pickup_location_id INT,
            dropoff_location_id INT,
            num_trips BIGINT,
            PRIMARY KEY (pickup_location_id, dropoff_location_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_taxi_trip_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            vendor_id STRING,
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            pickup_location_id INT,
            dropoff_location_id INT,
            passenger_count INT,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            PRIMARY KEY (lpep_pickup_datetime) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(5)
    ).with_timestamp_assigner(lambda event, timestamp: event["lpep_dropoff_datetime"])

    try:
        source_table = create_taxi_trip_source_kafka(t_env)
        aggregated_table = create_taxi_trip_sink(t_env)

        t_env.execute_sql(
            f"""
        INSERT INTO {aggregated_table}
        SELECT
            pickup_location_id,
            dropoff_location_id,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(
                TABLE {source_table},
                DESCRIPTOR(lpep_dropoff_datetime),
                INTERVAL '5' MINUTE
            )
        )
        GROUP BY pickup_location_id, dropoff_location_id
        ORDER BY num_trips DESC;
        """
        ).wait()

    except Exception as e:
        print("Error while processing taxi trips:", str(e))


if __name__ == "__main__":
    log_aggregation()
