-- Creacion de dimension para dias festivos
CREATE TABLE dim_dias_festivos (
    festivo_id INT IDENTITY(1,1) PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
	mes INT NOT NULL,
    dia INT NOT NULL,
    descripcion VARCHAR(100) NOT NULL
);

INSERT INTO dim_dias_festivos (fecha, mes, dia, descripcion)
VALUES
('2024-01-01', 1, 1, 'Año Nuevo'),
('2024-05-01', 5, 1, 'Día del Trabajo'),
('2024-06-30', 6, 30, 'Día del Ejército'),
('2024-09-15', 9, 15, 'Día de la Independencia'),
('2024-10-20', 10, 20, 'Revolución'),
('2024-11-01', 11, 1, 'Día de Todos los Santos'),
('2024-12-24', 12, 24, 'Nochebuena'),
('2024-12-25', 12, 25'Navidad'),
('2024-12-31', 12, 31, 'Año Viejo');

-- Adicion de nuevas columnas a la tabla de hechos
ALTER TABLE fact_flights ADD
    retraso_total INT NULL,
    semana INT NULL,
    trimestre INT NULL,
    dia_festivo INT NULL;

ALTER TABLE fact_flights
ADD CONSTRAINT FK_fact_festivo
FOREIGN KEY (dia_festivo)
REFERENCES dim_dias_festivos(festivo_id);

-- Creacion de valores derivados
UPDATE fact_flights
SET retraso_total = ISNULL(retraso_salida, 0) + ISNULL(retraso_llegada, 0);

UPDATE f
SET semana = DATEPART(WEEK, d.full_date)
FROM fact_flights f
JOIN dim_date d ON f.fecha_id = d.date_id;

UPDATE f
SET trimestre = DATEPART(QUARTER, d.full_date)
FROM fact_flights f
JOIN dim_date d ON f.fecha_id = d.date_id;

UPDATE f
SET festivo_id = h.festivo_id
FROM fact_flights f
JOIN dim_date d 
    ON f.fecha_id = d.date_id
JOIN dim_dias_festivos h
    ON MONTH(d.full_date) = h.mes
   AND DAY(d.full_date)  = h.dia;

UPDATE f
SET dia_festivo = h.festivo_id
FROM fact_flights f
JOIN dim_date d 
    ON f.fecha_id = d.date_id
JOIN dim_dias_festivos h
    ON MONTH(d.full_date) = h.mes
   AND DAY(d.full_date)  = h.dia;