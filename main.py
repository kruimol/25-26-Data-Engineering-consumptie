from db import get_engine
from pipelines.elia import run_elia_pipeline
from pipelines.energie_vlaanderen import run_vlaanderen_pipeline

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
    
    print("\nAlle pipelines zijn afgerond!")