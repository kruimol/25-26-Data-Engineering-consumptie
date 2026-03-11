from sqlalchemy import text

def run_combine_pipeline(engine):
    print("\n--- Start Data Combinatie Pipeline ---")
    
    # We bouwen de master tabel op basis van de Energie Vlaanderen data die we wél al hebben.
    # De Elia en Kaggle kolommen maken we alvast aan als 'leeg' (NULL).
    
    sql_query = """
    DROP TABLE IF EXISTS master_energy_table;
    
    CREATE TABLE master_energy_table AS
    WITH ev_solar AS (
        -- Tel alle megawatts van zon samen per uur (voor heel Vlaanderen, minus Antwerpen)
        SELECT 
            DATE_TRUNC('hour', datumtijd::timestamp) AS tijd, 
            SUM(vermogen_mw) AS ev_zon_mw
        FROM vlaanderen_energie_solar
        GROUP BY DATE_TRUNC('hour', datumtijd::timestamp)
    ),
    ev_wind AS (
        -- Tel alle megawatts van wind samen per uur
        SELECT 
            DATE_TRUNC('hour', datumtijd::timestamp) AS tijd, 
            SUM(vermogen_mw) AS ev_wind_mw
        FROM vlaanderen_energie_wind
        GROUP BY DATE_TRUNC('hour', datumtijd::timestamp)
    ),
    all_hours AS (
        -- Maak een unieke lijst van alle uren die in de dataset zitten
        SELECT tijd FROM ev_solar
        UNION
        SELECT tijd FROM ev_wind
    )
    
    -- Voeg alles samen in jouw gewenste layout!
    SELECT 
        h.tijd,
        s.ev_zon_mw AS "Energie vlaanderen zon",
        w.ev_wind_mw AS "Energie vlaanderen wind",
        NULL::numeric AS "Elia totaal",
        NULL::numeric AS "kaggle prive",
        NULL::numeric AS "kaggle openbaar"
    FROM all_hours h
    LEFT JOIN ev_solar s ON h.tijd = s.tijd
    LEFT JOIN ev_wind w ON h.tijd = w.tijd
    ORDER BY h.tijd ASC;
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(sql_query))
        print("  SUCCES: Tabel 'master_energy_table' is succesvol aangemaakt met de Energie Vlaanderen data!")
        print("  (De kolommen voor Elia en Kaggle staan klaar en zijn momenteel leeg).")
    except Exception as e:
        print(f"  FOUT bij het samenvoegen van de tabellen: {e}")