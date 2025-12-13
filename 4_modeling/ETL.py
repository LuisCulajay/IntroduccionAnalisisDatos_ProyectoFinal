from datetime import datetime
import pandas as pd
import pyodbc
import warnings

# ============================================================
# === CONFIGURACIÓN
# ============================================================

warnings.filterwarnings("ignore", category=UserWarning)

airlines_csv = r"datos/aerolineas.csv"
flights_csv = r"datos/vuelos.csv"  # archivo de 10GB

chunk_size = 500_000  # medio millón de filas por chunk

connection_string = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=DWBTS;"
    "Trusted_Connection=yes;"
)

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# ============================================================
# === CARGAR DIMENSION AEROLÍNEAS
# ============================================================

def cargar_dim_airline():
    df = pd.read_csv(airlines_csv)

    df["Code"] = df["Code"].astype(str).str.strip().str[:10]
    df["Description"] = df["Description"].astype(str).str.strip().str[:100]

    lista = df[["Code", "Description"]].values.tolist()

    cursor.executemany(
        "INSERT INTO dim_airline (code, description) VALUES (?, ?)",
        lista
    )
    conn.commit()
    print("✔ dim_airline cargado.")


# ============================================================
# === ETAPA 1: CARGAR DIM_DATE Y DIM_AIRPORT DESDE CHUNKS
# ============================================================

def procesar_dimensiones():
    print("Procesando dimensiones desde archivo de 10 GB...")

    fechas = set()
    aeropuertos = set()
    chunk_number = 0

    for chunk in pd.read_csv(
        flights_csv,
        chunksize=chunk_size,
        usecols=[
            "YEAR", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK",
            "ORIGIN_AIRPORT_ID", "ORIGIN_CITY_NAME", "ORIGIN_STATE_NM",
            "DEST_AIRPORT_ID", "DEST_CITY_NAME", "DEST_STATE_NM"
        ]
    ):

        chunk_number += 1
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Procesando chunk {chunk_number}...")

        # === DIM_DATE (NO requiere transformar)
        fechas.update(
            list(
                zip(
                    chunk["YEAR"].astype(int),
                    chunk["MONTH"].astype(int),
                    chunk["DAY_OF_MONTH"].astype(int),
                    chunk["DAY_OF_WEEK"].astype(int),
                )
            )
        )

        # === DIM_AIRPORT
        chunk["ORIGIN_CITY_NAME"] = chunk["ORIGIN_CITY_NAME"].astype(str).str.split(",", n=1).str[0]
        chunk["DEST_CITY_NAME"] = chunk["DEST_CITY_NAME"].astype(str).str.split(",", n=1).str[0]

        orig = list(
            zip(
                chunk["ORIGIN_AIRPORT_ID"].astype(str),
                chunk["ORIGIN_CITY_NAME"].astype(str),
                chunk["ORIGIN_STATE_NM"].astype(str)
            )
        )
        dest = list(
            zip(
                chunk["DEST_AIRPORT_ID"].astype(str),
                chunk["DEST_CITY_NAME"].astype(str),
                chunk["DEST_STATE_NM"].astype(str)
            )
        )

        aeropuertos.update(orig)
        aeropuertos.update(dest)

    # Convertir sets a listas
    fechas = list(fechas)
    aeropuertos = list(aeropuertos)

    print(f"✔ Fechas únicas encontradas: {len(fechas)}")
    print(f"✔ Aeropuertos únicos encontrados: {len(aeropuertos)}")

    # === INSERTAR DIM_DATE
    fechas_df = pd.DataFrame(fechas, columns=["year", "month", "day_of_month", "day_of_week"])
    fechas_df["full_date"] = pd.to_datetime(
        dict(year=fechas_df.year, month=fechas_df.month, day=fechas_df.day_of_month)
    )

    cursor.executemany(
        """INSERT INTO dim_date (year, month, day_of_month, day_of_week, full_date)
           VALUES (?, ?, ?, ?, ?)""",
        fechas_df.values.tolist()
    )
    conn.commit()

    # === INSERTAR DIM_AIRPORT
    airports_df = pd.DataFrame(
        aeropuertos,
        columns=["code", "city_name", "state_name"]
    )

    cursor.executemany(
        "INSERT INTO dim_airport (code, city_name, state_name) VALUES (?, ?, ?)",
        airports_df.values.tolist()
    )
    conn.commit()

    print("✔ Dimensiones cargadas correctamente.\n")


# ============================================================
# === ETAPA 2: CARGAR FACT_FLIGHTS POR CHUNKS
# ============================================================

def cargar_fact_flights():

    print("Cargando fact_flights por chunks...")

    # Cargar dimensiones una sola vez
    dim_airline = pd.read_sql("SELECT airline_id, code FROM dim_airline", conn)
    dim_airport = pd.read_sql("SELECT airport_id, code FROM dim_airport", conn)
    dim_date = pd.read_sql(
        "SELECT date_id, year, month, day_of_month FROM dim_date", conn
    )

    for chunk in pd.read_csv(flights_csv, chunksize=chunk_size):

        # Preparar joins
        dim_airline["code"] = dim_airline["code"].astype(str)
        chunk["OP_UNIQUE_CARRIER"] = chunk["OP_UNIQUE_CARRIER"].astype(str)

        # MERGE airline_id
        chunk = chunk.merge(dim_airline, left_on="OP_UNIQUE_CARRIER", right_on="code", how="left")
        chunk = chunk.rename(columns={"airline_id": "airline_id_fk"}).drop(columns=["code"])

        # MERGE aeropuertos
        chunk["ORIGIN_AIRPORT_ID"] = chunk["ORIGIN_AIRPORT_ID"].astype(str)
        chunk["DEST_AIRPORT_ID"] = chunk["DEST_AIRPORT_ID"].astype(str)
        dim_airport["code"] = dim_airport["code"].astype(str)

        chunk = chunk.merge(dim_airport, left_on="ORIGIN_AIRPORT_ID", right_on="code", how="left")
        chunk = chunk.rename(columns={"airport_id": "origin_airport_id_fk"}).drop(columns=["code"])

        chunk = chunk.merge(dim_airport, left_on="DEST_AIRPORT_ID", right_on="code", how="left")
        chunk = chunk.rename(columns={"airport_id": "destination_airport_id_fk"}).drop(columns=["code"])

        # MERGE fecha
        chunk = chunk.merge(
            dim_date,
            left_on=["YEAR", "MONTH", "DAY_OF_MONTH"],
            right_on=["year", "month", "day_of_month"],
            how="left"
        ).rename(columns={"date_id": "fecha_id_fk"})

        # Selección final
        fact_chunk = chunk[
            [
                "airline_id_fk",
                "origin_airport_id_fk",
                "destination_airport_id_fk",
                "fecha_id_fk",
                "DEP_DELAY",
                "ARR_DELAY",
                "CANCELLED",
                "DIVERTED",
                "CARRIER_DELAY",
                "WEATHER_DELAY",
                "NAS_DELAY",
                "SECURITY_DELAY",
                "LATE_AIRCRAFT_DELAY"
            ]
        ].fillna(0)

        # Insertar batch
        cursor.executemany(
            """
            INSERT INTO fact_flights (
                airline_id,
                origin_airport_id,
                destination_airport_id,
                fecha_id,
                retraso_salida,
                retraso_llegada,
                cancelado,
                desviado,
                carrier_delay,
                weather_delay,
                NAS_delay,
                security_delay,
                aircraft_delay
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            fact_chunk.values.tolist()
        )
        conn.commit()

        print(f"✔ Chunk insertado ({len(fact_chunk)} registros)")

    print("✔ fact_flights cargado completamente.\n")

# ============================================================
# === EJECUCIÓN
# ============================================================

cargar_dim_airline()
procesar_dimensiones()
cargar_fact_flights()

cursor.close()
conn.close()

print("ETL FINALIZADO CORRECTAMENTE.")