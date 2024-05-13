import pandas as pd

# Load the Parquet file into a DataFrame
df = pd.read_parquet(r'D:\projectos\programacion\henrry\bootcamp\proyecto final\Projecto_final_henry\DATASETS\historical_wheather_data_2000_2020_raw.parquet')

# Convert the DataFrame to a CSV file
df.to_csv('Historical_Weather_clean.csv', index=False)