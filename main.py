from db import get_engine
from pipelines.elia import run_elia_pipeline
from pipelines.energie_vlaanderen import run_vlaanderen_pipeline
from pipelines.combine_data import run_combine_pipeline
from pipelines.kaggle import run_kaggle_pipeline
from pipelines.export_csv import export_all_tables_to_csv
from pipelines.export_azure import export_master_to_azure

if __name__ == "__main__":
    print("Test verbinding met database...")
    try:
        engine = get_engine()
        print("Verbinding OK!\n")
    except Exception as e:
        print(f"Database verbinding mislukt: {e}")
        exit(1)

    # Voer de pipelines uit
    run_elia_pipeline(engine)
    run_vlaanderen_pipeline(engine)
    run_kaggle_pipeline(engine)

    # Combineer alles tot één master_energy_table in de lokale DB
    run_combine_pipeline(engine)
    
    # Exporteer data naar CSV (optioneel, kun je laten staan of weghalen)
    export_all_tables_to_csv(engine)
    
    # NIEUW: Exporteer de master tabel naar Azure
    # export_master_to_azure(engine)
    
    print("\nAlle pipelines zijn afgerond!")