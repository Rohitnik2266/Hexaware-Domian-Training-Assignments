import pandas as pd
import numpy as np
from helpers import parse_amount, parse_date

df = pd.read_csv('data/raw/expenses.csv')

def parse_amount(x):
    return float(str(x).replace('$', '').replace(',', '').strip())

def parse_date(x):
    return pd.to_datetime(x, errors='coerce')
    
df['Amount'] = df['Amount'].apply(parse_amount)

df['Date'] = df['Date'].apply(parse_date)

df['Month'] = df['Date'].dt.to_period('M')

# Save to CSV
df.to_csv('data/cleaned/expenses_cleaned.csv', index=False)
print("Cleaned data saved to 'data/cleaned/expenses_cleaned.csv'")


df = pd.read_csv('data/cleaned/expenses_cleaned.csv')

#Use numpy to calculate monthly totals and averages
df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M')
breakdown = df.groupby(['Month', 'CategoryID'])['Amount'].sum().unstack().fillna(0)

monthly_totals = df.groupby('Month')['Amount'].sum()

monthly_avg = df.groupby('UserID')['Amount'].mean()

print("Category-wise monthly breakdown:\n", breakdown)
print("\n Monthly Totals:\n", monthly_totals)
print("\n Average expense per user:\n", monthly_avg)

# Save to CSV
breakdown.to_csv('data/cleaned/category_monthly_wise.csv')
