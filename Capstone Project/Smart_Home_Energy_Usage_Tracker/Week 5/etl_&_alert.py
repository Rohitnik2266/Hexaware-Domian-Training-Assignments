import pandas as pd

df = pd.read_csv("PySpark_Aggregation/top_uasage.csv")

THRESHOLD = 10.0

alerts = df[df['total_kwh'] > THRESHOLD]

alerts.to_csv("DevOps/energy_log.csv", index=False)

if not alerts.empty:
    print("⚠️ Devices exceeding threshold:")
    print(alerts)
else:
    print("✅ All devices within safe energy usage.")
