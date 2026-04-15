import pandas as pd
import gspread
import urllib
import numpy as np
import os
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, text

# ─────────────────────────────
# GOOGLE SHEETS CONNECTION
# ─────────────────────────────
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "citifuel-387a0ec421a9.json", scope
)

client = gspread.authorize(creds)

sheet = client.open_by_key("1EzR2IDRpu7NFfzy_4VLbkEwf0haenJ18_x3KR5lpRVQ")
worksheet = sheet.worksheet("Maintenance")

data = worksheet.get_all_records()
df = pd.DataFrame(data)

# ─────────────────────────────
# DATA CLEANING
# ─────────────────────────────
df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)

df['Customer Service Member'] = df['Customer Service Member'].str.strip()

allowed_names = [
    "Tim", "David", "Mason", "Mike", "Eddy", "Alan",
    "Jeff", "Cassie", "Jane", "Daniel", "Tomas",
    "Dave", "Denis", "Johnson", "Patrick", "Frank"
]

df = df[df['Customer Service Member'].isin(allowed_names)]

# ─────────────────────────────
# POINT SYSTEM
# ─────────────────────────────
service_points = {
    "Eats": 1,
    "Truck Parking": 1,
    "Truck Wash": 1,
    "Parts purchase": 2,
    "Tire Replacement": 2,
    "Tire repair": 1,
    "PMs": 2,
    "DOT inspection": 2.5,
    "Dealership": 3.5,
    "RS/Tire Replacement": 4,
    "RS/Mechanical": 6,
    "Towing": 6,
    "Tire Replacement/PMs": 3,
    "Mechanical": 3,
    "Diagnosting": 3,
    "PMs/Mechanical": 5,
    "Tire Replacement/Mechanical": 5,
    "RS": 4
}

df["Point"] = df["Service type"].map(service_points)

# ─────────────────────────────
# COMPANY BONUS
# ─────────────────────────────
company_df = pd.read_csv('company list.csv', encoding='latin1')
company_set = set(company_df["List of Companies"].str.lower().str.strip())

df["is_in_company_list"] = (
    df["Truck Stop"].str.lower().str.strip()
    .isin(company_set)
    .map({True: 0.75, False: 0.25})
)

df['Bonus'] = np.where(
    df['Status'] == 'In CMP',
    df['Point'] * df['is_in_company_list'],
    0
)

# ─────────────────────────────
# DATA CLEANING (BAD VALUES)
# ─────────────────────────────
card_mask = df['Card number'].apply(lambda x: isinstance(x, str) and not x.strip().isdigit())

def is_bad_mn(val):
    if pd.isna(val):
        return False
    return not str(val).strip().replace('.', '', 1).isdigit()

mn_mask = df['MN card'].apply(is_bad_mn)

cs_mask = df['Case source'].apply(lambda x: str(x).strip() == '#REF!' if pd.notna(x) else False)

df.loc[card_mask, 'Card number'] = np.nan
df.loc[mn_mask, 'MN card'] = np.nan
df.loc[cs_mask, 'Case source'] = np.nan

# ─────────────────────────────
# AGENT GRADES
# ─────────────────────────────
agent_grades = {
    'David':'A','Mason':'A','Mike':'A','Eddy':'A','Alan':'B',
    'Jeff':'A','Cassie':'C','Jane':'C','Daniel':'C','Tomas':'A',
    'Dave':'B','Denis':'A','Johnson':'C','Patrick':'C','Frank':'C','Tim':'B'
}

df['Grade'] = df['Customer Service Member'].map(agent_grades)

# ─────────────────────────────
# SERVICE POINTS (A/B/C)
# ─────────────────────────────
service_points = {
    'Eats': (1.0,1.0,1.0),
    'Truck Parking': (0.0,1.0,2.0),
    'Truck Wash': (0.0,1.0,2.0),
    'Parts purchase': (1.0,2.0,3.0),
    'Tire Replacement': (1.0,1.5,2.0),
    'Tire repair': (0.0,1.5,2.0),
    'PMs': (0.5,1.0,2.0),
    'DOT inspection': (1.0,2.0,3.0),
    'Dealership': (1.5,2.5,4.0),
    'RS/Tire Replacement': (2.0,4.0,6.0),
    'RS/Mechanical': (4.0,6.0,8.0),
    'Towing': (4.0,7.0,8.0),
    'Tire Replacement/PMs': (1.0,2.0,4.0),
    'Mechanical': (2.0,3.0,5.0),
    'Diagnosting': (2.0,3.0,5.0),
    'PMs/Mechanical': (4.0,7.0,8.0),
    'Tire Replacement/Mechanical': (4.0,7.0,8.0)
}

df['Points_A'] = df['Service type'].map(lambda x: service_points.get(x, (np.nan,np.nan,np.nan))[0])
df['Points_B'] = df['Service type'].map(lambda x: service_points.get(x, (np.nan,np.nan,np.nan))[1])
df['Points_C'] = df['Service type'].map(lambda x: service_points.get(x, (np.nan,np.nan,np.nan))[2])

id_cols = [
    "Invoice Number",
    "Card number",
    "MN card",
    "Truck number",
    "EFS Report Reference",
    "№"
]

for col in id_cols:
    if col in df.columns:
        df[col] = df[col].astype(str)

# connecting postgresql

db_url = os.environ["DATABASE_URL"]

engine = create_engine(db_url)

print("Table truncated successfully.")

with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS FleetPerformance"))

    conn.execute(text("""
    CREATE TABLE FleetPerformance (
        "№" TEXT,
        "Date" DATE,
        "Maintenance Account" TEXT,
        "EFS Report Reference" TEXT,
        "National tire account" TEXT,
        "Invoice Number" TEXT,
        "PO" TEXT,
        "Status" TEXT,
        "Customer Service Member" TEXT,
        "Sales Agent" TEXT,
        "Card number" TEXT,
        "MN card" TEXT,
        "Driver Name" TEXT,
        "Truck number" TEXT,
        "Truck Stop" TEXT,
        "Company" TEXT,
        "Service type" TEXT,
        "Brand type" TEXT,
        "Invoice" TEXT,
        "Customer discount" TEXT,
        "Our comission" TEXT,
        "Transaction Fee" TEXT,
        "Case source" TEXT,
        "Comments" TEXT,
        "Number of Tires" TEXT,
        "Fee Start Date" TEXT,
        "Point" DOUBLE PRECISION,
        is_in_company_list DOUBLE PRECISION,
        "Bonus" DOUBLE PRECISION,
        "Grade" TEXT,
        "Points_A" DOUBLE PRECISION,
        "Points_B" DOUBLE PRECISION,
        "Points_C" DOUBLE PRECISION
        )
    """))

df.to_sql(
    name="FleetPerformance",
    con=engine,
    if_exists="append",
    index=False
)

print(f"✅ Full reload completed. Inserted {len(df)} rows.")
