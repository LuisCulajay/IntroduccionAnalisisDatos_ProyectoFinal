import pandas as pd
import pyodbc

# === CONFIGURACIÓN ===
airlines_csv = r"datos/aerolineas.csv"
flights_csv = r"datos/muestra_vuelos_2011.csv"   # <-- archivo donde vienen YEAR, MONTH, DAY_OF_MONTH, DAY_OF_WEEK

connection_string = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=DWBTS;"
    "Trusted_Connection=yes;"
)

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# ============================================================
# === CARGA DE TABLA EXTERNA (AEROLINEAS)
# ============================================================
def cargar_dim_airline():
    df_airlines = pd.read_csv(airlines_csv)

    df_airlines["Code"] = df_airlines["Code"].astype(str).str.strip()
    df_airlines["Description"] = df_airlines["Description"].astype(str).str.strip()

    df_airlines["Code"] = df_airlines["Code"].apply(lambda x: x[:10] if isinstance(x, str) else x)
    df_airlines["Description"] = df_airlines["Description"].apply(lambda x: x[:100] if isinstance(x, str) else x)

    airlines_list = df_airlines[["Code", "Description"]].values.tolist()

    print(f"Cargando {len(airlines_list)} aerolíneas...")

    try:
        cursor.executemany(
            """
            INSERT INTO dim_airline (code, description)
            VALUES (?, ?)
            """,
            airlines_list
        )
        conn.commit()
        print("Carga de aerolíneas completada correctamente.\n")

    except Exception as e:
        print("Error en carga de aerolíneas:", e)
        conn.rollback()

# ============================================================
# === CARGA DE FECHAS HACIA dim_date
# ============================================================
def cargar_dim_date():
    print("Procesando fechas únicas para dim_date...")

    df_dates = pd.read_csv(
        flights_csv,
        usecols=["YEAR", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK"]
    )

    df_dates = df_dates.astype(int)
    df_dates_unique = df_dates.drop_duplicates()

    df_dates_unique = df_dates_unique.copy()
    df_dates_unique["fullDate"] = pd.to_datetime(
        dict(
            year=df_dates_unique["YEAR"],
            month=df_dates_unique["MONTH"],
            day=df_dates_unique["DAY_OF_MONTH"]
        )
    )

    df_dates_unique = df_dates_unique.sort_values("fullDate")

    dates_list = df_dates_unique[
        ["YEAR", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK", "fullDate"]
    ].values.tolist()

    print(f"Fechas encontradas: {len(dates_list)}")

    try:
        cursor.executemany(
            """
            INSERT INTO dim_date (year, month, day_of_month, day_of_week, full_date)
            VALUES (?, ?, ?, ?, ?)
            """,
            dates_list
        )
        conn.commit()
        print("Carga de fechas completada correctamente.\n")

    except Exception as e:
        print("Error en carga de fechas:", e)
        conn.rollback()


# # ============================================================
# # === CARGA DE AEROPUERTOS HACIA dim_airport
# # ============================================================
def cargar_dim_airport():
    print("Procesando aeropuertos únicos para dim_airport...")

    # Cargar columnas necesarias del archivo de vuelos
    df_airports = pd.read_csv(
        flights_csv,
        usecols=[
            "ORIGIN_AIRPORT_ID", "ORIGIN_CITY_NAME", "ORIGIN_STATE_NM",
            "DEST_AIRPORT_ID", "DEST_CITY_NAME", "DEST_STATE_NM"
        ]
    )

    # ===== Limpiar el nombre de la ciudad: quitar texto después de la coma =====
    df_airports["ORIGIN_CITY_NAME"] = (
        df_airports["ORIGIN_CITY_NAME"]
        .astype(str)
        .str.split(",", n=1, expand=True)[0]
        .str.strip()
    )

    df_airports["DEST_CITY_NAME"] = (
        df_airports["DEST_CITY_NAME"]
        .astype(str)
        .str.split(",", n=1, expand=True)[0]
        .str.strip()
    )

    # ===== Construir tabla ORIGEN =====
    orig = df_airports[
        ["ORIGIN_AIRPORT_ID", "ORIGIN_CITY_NAME", "ORIGIN_STATE_NM"]
    ].rename(columns={
        "ORIGIN_AIRPORT_ID": "code",
        "ORIGIN_CITY_NAME": "city_name",
        "ORIGIN_STATE_NM": "state_name"
    })

    # ===== Construir tabla DESTINO =====
    dest = df_airports[
        ["DEST_AIRPORT_ID", "DEST_CITY_NAME", "DEST_STATE_NM"]
    ].rename(columns={
        "DEST_AIRPORT_ID": "code",
        "DEST_CITY_NAME": "city_name",
        "DEST_STATE_NM": "state_name"
    })

    # Unificar origen + destino
    df_airports_final = pd.concat([orig, dest], ignore_index=True)

    # Quitar duplicados
    df_airports_final = df_airports_final.drop_duplicates()

    # Limpiar espacios
    df_airports_final["code"] = df_airports_final["code"].astype(str).str.strip()
    df_airports_final["city_name"] = df_airports_final["city_name"].astype(str).str.strip()
    df_airports_final["state_name"] = df_airports_final["state_name"].astype(str).str.strip()

    # Convertir a tuplas
    airports_list = df_airports_final[["code", "city_name", "state_name"]].values.tolist()

    print(f"Aeropuertos únicos encontrados: {len(airports_list)}")

    try:
        cursor.executemany(
            """
            INSERT INTO dim_airport (code, city_name, state_name)
            VALUES (?, ?, ?)
            """,
            airports_list
        )
        conn.commit()
        print("Carga de aeropuertos completada correctamente.\n")

    except Exception as e:
        print("Error en carga de aeropuertos:", e)
        conn.rollback()

# cargar_dim_airline()
# cargar_dim_date()
# cargar_dim_airport()

cursor.close()
conn.close()

print("ETL finalizó correctamente.")