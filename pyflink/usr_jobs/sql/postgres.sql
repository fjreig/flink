-- Tablas Source kafka
CREATE TABLE FV_Pavasal_Cheste_EMI (
    Fecha STRING,
    Radiacion FLOAT,
    ts_ltz AS TO_TIMESTAMP(Fecha),
    WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'FV_Pavasal_Cheste_EMI',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'test-group',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);

CREATE TABLE FV_Pavasal_Cheste_Inv (
    Fecha STRING,
    PA_Inv FLOAT,
    ts_ltz AS TO_TIMESTAMP(Fecha),
    WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'FV_Pavasal_Cheste_Inv',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'test-group',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);

-- Tablas Temporales
CREATE TEMPORARY VIEW FV_Pavasal_Cheste_emi_intermedia AS 
  SELECT window_start, window_end, round(avg(Radiacion),1) as Radiacion
    from table(TUMBLE(table FV_Pavasal_Cheste_EMI, descriptor(ts_ltz), INTERVAL '10' MINUTES)) group by window_start, window_end;

CREATE TEMPORARY VIEW FV_Pavasal_Cheste_inv_intermedia AS 
  SELECT window_start, window_end, round(avg(PA_Inv),1) as PA
    from table(TUMBLE(table FV_Pavasal_Cheste_Inv, descriptor(ts_ltz), INTERVAL '10' MINUTES)) group by window_start, window_end;

-- Tablas Sink Postgres
CREATE TABLE FV_Pavasal_Cheste_Final WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/monitorizacion',
    'table-name' = 'fv_pavasal_cheste',
    'username' = 'postgres',
    'password' = 'postgres'
) AS
    SELECT e.window_start, e.window_end, round(e.Radiacion, 1) as Radiacion, round(i.PA, 1) as PA 
        FROM FV_Pavasal_Cheste_emi_intermedia e
        INNER JOIN FV_Pavasal_Cheste_inv_intermedia i
        ON e.window_start = i.window_start;
