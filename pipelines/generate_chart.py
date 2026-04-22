import pandas as pd
import matplotlib.pyplot as plt
from config import DATA_DIR
import os

def generate_time_series_chart():
    """Reads the exported master_energy_table.csv and generates multiple time series charts."""
    print("Start generating enhanced time series charts...")
    
    csv_path = DATA_DIR / "exports" / "master_energy_table.csv"
    if not csv_path.exists():
        print(f"  Geen CSV gevonden op {csv_path}. Kan geen grafieken maken.")
        return
        
    try:
        # Lees de CSV in en zet 'tijd' als datetime index
        df = pd.read_csv(csv_path)
        df['tijd'] = pd.to_datetime(df['tijd'])
        df.set_index('tijd', inplace=True)
        
        # Resamplen naar dagelijkse gemiddeldes
        df_daily = df.resample('D').mean()
        
        # Groepen definiëren
        vlaanderen_cols = ['Energie vlaanderen zon', 'Energie vlaanderen wind']
        kaggle_cols = ['kaggle prive', 'kaggle openbaar']
        elia_cols = ['Elia totaal']
        
        def save_plot(data, columns, title, filename):
            available_cols = [c for c in columns if c in data.columns]
            if not available_cols:
                return
                
            plt.figure(figsize=(14, 7))
            for col in available_cols:
                plt.plot(data.index, data[col], label=col, linewidth=1.5)
            
            plt.title(title, fontsize=16)
            plt.xlabel('Tijd', fontsize=12)
            plt.ylabel('Energie', fontsize=12)
            plt.legend(loc='upper right', fontsize=10)
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.tight_layout()
            
            output_path = DATA_DIR / "exports" / filename
            plt.savefig(output_path, dpi=300)
            plt.close()
            print(f"  ✓ Grafiek opgeslagen: {filename}")

        # 1. Algemene grafiek (alles)
        save_plot(df_daily, df_daily.columns, 'Totale Energie Consumptie/Productie (Dagelijks Gemiddelde)', 'energy_chart_total.png')
        
        # 2. Vlaanderen apart
        save_plot(df_daily, vlaanderen_cols, 'Energie Vlaanderen: Zon & Wind (Dagelijks Gemiddelde)', 'energy_chart_vlaanderen.png')
        
        # 3. Kaggle apart
        save_plot(df_daily, kaggle_cols, 'Kaggle Energieverbruik: Prive & Openbaar (Dagelijks Gemiddelde)', 'energy_chart_kaggle.png')
        
        # 4. Elke kolom apart
        for col in df_daily.columns:
            filename = f"energy_chart_col_{col.lower().replace(' ', '_')}.png"
            save_plot(df_daily, [col], f'Energieverbruik: {col} (Dagelijks Gemiddelde)', filename)
        
        # 5. Grafieken per jaar
        years = df_daily.index.year.unique()
        for year in years:
            df_year = df_daily[df_daily.index.year == year]
            save_plot(df_year, df_year.columns, f'Energieverbruik Totaal - Jaar {year}', f'energy_chart_{year}.png')

        print("Alle grafieken succesvol gegenereerd!")
    except Exception as e:
        print(f"  ✗ Fout bij het genereren van de grafieken: {e}")

