import pandas as pd

alerts_path = "etl/databricks_notebook/monthly_alerts.csv"  
df = pd.read_csv(alerts_path)

if not df.empty:
    print("⚠️ ALERT: High spending detected. Email will be sent.")
else:
    print("✅ No unusual spending.")
