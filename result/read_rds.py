import pandas as pd
from sqlalchemy import create_engine, inspect

# Remplacez par votre mot de passe
db_url = "postgresql://postgres:S18102002b!@olist-db.crc46smg6jwf.eu-west-3.rds.amazonaws.com:5432/postgres"
engine = create_engine(db_url)

# Lecture des données
query = "SELECT * FROM olist_final_analytics"
df = pd.read_sql(query, engine)

print("--- APERÇU DES DONNÉES ---")
print(df[['product_category_name', 'review_score', 'is_high_risk']].head(10))

print("\n--- STATISTIQUES DE RISQUE ---")
print(df['is_high_risk'].value_counts(normalize=True) * 100)