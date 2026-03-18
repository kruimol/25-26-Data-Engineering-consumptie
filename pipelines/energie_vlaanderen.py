import pandas as pd
from datetime import datetime
from sqlalchemy import inspect  # <-- Toegevoegd
from config import VLAANDEREN_URLS, REFNIS_URL, FILTER_START, FILTER_END
from utils import daterange, fetch_csv_to_df
from db import write_to_db

# ... [fetch_refnis en melt_energy blijven ongewijzigd] ...

def fetch_refnis() -> pd.DataFrame:
    # ... (jouw originele code)
    print("  Downloading NIS-referentietabel ...")
    df = fetch_csv_to_df(REFNIS_URL, sep="|")
    
    df = df[["Code NIS", "Administratieve eenheden"]].copy()
    df = df.dropna(subset=["Code NIS"])
    df = df.rename(columns={"Code NIS": "nis_code", "Administratieve eenheden": "gemeente"})
    
    df["nis_code"] = pd.to_numeric(df["nis_code"], errors="coerce")
    df = df.dropna(subset=["nis_code"])
    df["nis_code"] = df["nis_code"].astype(int)
    df["gemeente"] = df["gemeente"].str.strip()
    return df.drop_duplicates(subset=["nis_code"])

def melt_energy(df: pd.DataFrame, energy_type: str) -> pd.DataFrame:
    # ... (jouw originele code)
    dt_col = df.columns[0]
    nis_cols = df.columns[1:]

    melted = df.melt(
        id_vars=[dt_col],
        value_vars=nis_cols,
        var_name="nis_code",
        value_name="vermogen_mw",
    )
    melted = melted.rename(columns={dt_col: "datumtijd"})
    melted["nis_code"] = pd.to_numeric(melted["nis_code"], errors="coerce")
    melted["type"] = energy_type
    return melted

def run_vlaanderen_pipeline(engine, force_reload=False):  # <-- Parameter toegevoegd
    print("\n--- Start Energie Vlaanderen Pipeline ---")
    
    # Check of de data al bestaat
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    target_tables = [f"vlaanderen_energie_{k}" for k in VLAANDEREN_URLS.keys()]
    
    if not force_reload and all(t in existing_tables for t in target_tables):
        print("  Alle Energie Vlaanderen tabellen bestaan al in de DB. Skipping download.")
        return

    start_date = datetime.strptime(FILTER_START, "%Y-%m-%d").date()
    end_date = datetime.strptime(FILTER_END, "%Y-%m-%d").date()

    if start_date > end_date:
        print("  Fout: Startdatum moet voor of op de einddatum liggen.")
        return

    refnis = fetch_refnis()
    frames = []

    # 1. Extract (Ophalen per dag)
    for day in daterange(start_date, end_date):
        date_str = day.strftime("%Y%m%d")
        print(f"\n  Ophalen data voor: {day}")

        for label, url_template in VLAANDEREN_URLS.items():
            url = url_template.format(date=date_str)
            df = fetch_csv_to_df(url)
            
            if df is None:
                print(f"    {label}: niet beschikbaar")
                continue
                
            melted = melt_energy(df, label)
            frames.append(melted)
            print(f"    {label}: {len(df)} tijdstippen, {len(df.columns) - 1} gemeenten")

    if not frames:
        print("  Geen data gedownload voor deze periode.")
        return

    # 2. Transform (Samenvoegen en Filteren)
    combined = pd.concat(frames, ignore_index=True)
    
    # Koppel met gemeentenamen
    combined = combined.merge(refnis, on="nis_code", how="left")
    combined["gemeente"] = combined["gemeente"].fillna(combined["nis_code"].astype(str))

    # --- JOUW FILTER: SLUIT ANTWERPEN UIT ---
    voor_filter = len(combined)
    combined = combined[combined["gemeente"].str.lower() != "antwerpen"]
    na_filter = len(combined)
    print(f"\n  Filter toegepast: {voor_filter - na_filter} rijen voor 'Antwerpen' verwijderd.")
    # ----------------------------------------

    # 3. Load (Schrijf naar de database per type)
    for energy_type in VLAANDEREN_URLS.keys():
        subset = combined[combined["type"] == energy_type]
        if subset.empty:
            continue
            
        table_name = f"vlaanderen_energie_{energy_type}"
        print(f"  Bezig met wegschrijven van {energy_type}...")
        write_to_db(engine, subset, table_name)