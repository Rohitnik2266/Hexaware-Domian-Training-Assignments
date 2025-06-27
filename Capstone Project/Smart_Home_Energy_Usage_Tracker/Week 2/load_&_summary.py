import pandas as pd 
import numpy as np

df = pd.read_csv("Data_Preprocessing/data/enegryusage.csv") 

df['timestamp'] = pd.to_datetime(df['timestamp']) 
df['energykwh'] = df['energy_kwh'].astype(float)

df_clean = df.dropna()

df_clean.to_csv('Data_Preprocessing/data/cleaned_energyusage.csv', index=False)

df_cleaned = pd.read_csv('Data_Preprocessing/data/cleaned_energyusage.csv')

device_summary = df.groupby('device_id')['energy_kwh'].agg(['sum', 'mean'])

room_summary = df.groupby('room_id')['energykwh'].sum()

print("Device Summary:\n", device_summary)
print("\nRoom Summary:\n", room_summary)