-- Zestaw 11 - flights-airports
-- Parametry: input_dir3, input_dir4, output_dir6 (przekazywane przez --hivevar)

USE default;

-- (3) wynik MapReduce
DROP TABLE IF EXISTS flights_monthly_stats;
CREATE EXTERNAL TABLE flights_monthly_stats (
    departure_airport_id STRING,
    departure_month      STRING,
    flight_count         BIGINT,
    avg_ticket_price     DOUBLE,
    avg_delay_min        DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir3}';

-- (4) datasource4 - lotniska (pola w cudzysłowach mogą zawierać przecinki)
DROP TABLE IF EXISTS airports;
CREATE EXTERNAL TABLE airports (
    airport_id   STRING,
    airport_name STRING,
    city         STRING,
    country      STRING,
    continent    STRING,
    type         STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"'
)
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir4}'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- (6) wynik końcowy w formacie JSON (jeden rekord JSON na wiersz)
DROP TABLE IF EXISTS country_flights_ranking;
CREATE EXTERNAL TABLE country_flights_ranking (
    continent         STRING,
    country           STRING,
    total_flights     BIGINT,
    avg_ticket_price  DOUBLE,
    rank_in_continent INT,
    flights_deviation DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hivevar:output_dir6}';

INSERT OVERWRITE TABLE country_flights_ranking
SELECT
    continent,
    country,
    total_flights,
    avg_ticket_price,
    RANK() OVER (PARTITION BY continent ORDER BY total_flights DESC)   AS rank_in_continent,
    total_flights - AVG(total_flights) OVER (PARTITION BY continent)    AS flights_deviation
FROM (
    SELECT
        a.continent,
        a.country,
        SUM(f.flight_count) AS total_flights,
        -- średnia po wszystkich lotach kraju = suma cen / liczba lotów
        SUM(f.avg_ticket_price * f.flight_count) / SUM(f.flight_count) AS avg_ticket_price
    FROM flights_monthly_stats f
    JOIN airports a ON f.departure_airport_id = a.airport_id
    GROUP BY a.continent, a.country
) country_stats;
