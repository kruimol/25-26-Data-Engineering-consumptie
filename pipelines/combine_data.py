from sqlalchemy import text

def run_combine_pipeline(engine):
    print("\n--- Start Data Combinatie Pipeline ---")
    
    sql_query = """
    DROP TABLE IF EXISTS master_energy_table;
    
    CREATE TABLE master_energy_table AS
    WITH ev_solar AS (
        SELECT DATE_TRUNC('hour', datumtijd::timestamp) AS tijd, SUM(vermogen_mw) AS ev_zon_mw
        FROM vlaanderen_energie_solar GROUP BY DATE_TRUNC('hour', datumtijd::timestamp)
    ),
    ev_wind AS (
        SELECT DATE_TRUNC('hour', datumtijd::timestamp) AS tijd, SUM(vermogen_mw) AS ev_wind_mw
        FROM vlaanderen_energie_wind GROUP BY DATE_TRUNC('hour', datumtijd::timestamp)
    ),
    kaggle_district_hourly AS (
        -- Tel alle 'openbare' verbruiken op per uur en converteer kW naar MW (/1000)
        SELECT DATE_TRUNC('hour', time) AS tijd, SUM(total_calc) / 1000.0 AS kaggle_openbaar_mw
        FROM kaggle_district_raw GROUP BY DATE_TRUNC('hour', time)
    ),
    kaggle_private_hourly AS (
        -- Tel alle private verbruiken op per uur en converteer kW naar MW (/1000)
        SELECT DATE_TRUNC('hour', time) AS tijd, SUM(power_kw) / 1000.0 AS kaggle_prive_mw
        FROM kaggle_private_raw GROUP BY DATE_TRUNC('hour', time)
    ),
    all_hours AS (
        SELECT tijd FROM ev_solar
        UNION SELECT tijd FROM ev_wind
        UNION SELECT tijd FROM kaggle_district_hourly
        UNION SELECT tijd FROM kaggle_private_hourly
    )
    
    SELECT 
        h.tijd,
        s.ev_zon_mw AS "Energie vlaanderen zon",
        w.ev_wind_mw AS "Energie vlaanderen wind",
        NULL::numeric AS "Elia totaal", -- Nog leeg tot we Elia hebben
        kp.kaggle_prive_mw AS "kaggle prive",
        kd.kaggle_openbaar_mw AS "kaggle openbaar"
    FROM all_hours h
    LEFT JOIN ev_solar s ON h.tijd = s.tijd
    LEFT JOIN ev_wind w ON h.tijd = w.tijd
    LEFT JOIN kaggle_private_hourly kp ON h.tijd = kp.tijd
    LEFT JOIN kaggle_district_hourly kd ON h.tijd = kd.tijd
    ORDER BY h.tijd ASC;
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(sql_query))
        print("  SUCCES: Master tabel is geüpdatet met Energie Vlaanderen EN Kaggle data!")
    except Exception as e:
        print(f"  FOUT bij het samenvoegen: {e}")