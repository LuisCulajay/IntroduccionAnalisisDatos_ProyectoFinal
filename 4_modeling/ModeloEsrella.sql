CREATE DATABASE DWBTS;

USE DWBTS;

CREATE TABLE dim_airline (
    airline_id INT IDENTITY(1,1) PRIMARY KEY,
    code VARCHAR(10),
    description VARCHAR(200)
);

CREATE TABLE dim_date (
    date_id INT IDENTITY(1,1) PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    full_date DATE NOT NULL
);

CREATE TABLE dim_airport (
    airport_id INT IDENTITY(1,1) PRIMARY KEY,
    code VARCHAR(10) NOT NULL,
    city_name VARCHAR(100),
    state_name VARCHAR(100)
);

CREATE TABLE fact_flights (
    flight_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    airline_id INT NOT NULL,
    origin_airport_id INT NOT NULL,
    destination_airport_id INT NOT NULL,
    fecha_id INT NOT NULL,
    retraso_salida INT,
    retraso_llegada INT,
    cancelado BIT,
    desviado BIT,
    carrier_delay INT,
    weather_delay INT,
    NAS_delay INT,
    security_delay INT,
    aircraft_delay INT,
    -- columnas derivadas
    retraso_total INT NULL,
    categoria_de_retraso VARCHAR(20) NULL,
    semana INT NULL,
    trimestre INT NULL,
    dia_festivo INT NULL;
    -- llaves foraneas
    CONSTRAINT FK_fact_airline FOREIGN KEY (airline_id) REFERENCES dim_airline(airline_id),
    CONSTRAINT FK_fact_origin_airport FOREIGN KEY (origin_airport_id) REFERENCES dim_airport(airport_id),
    CONSTRAINT FK_fact_dest_airport FOREIGN KEY (destination_airport_id) REFERENCES dim_airport(airport_id),
    CONSTRAINT FK_fact_date FOREIGN KEY (fecha_id) REFERENCES dim_date(date_id)
);

 --DROP TABLE fact_flights;
 --DROP TABLE dim_airline;
 --DROP TABLE dim_airport;
 --DROP TABLE dim_date;