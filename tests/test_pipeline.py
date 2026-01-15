import pandas as pd

files = {
    "Ventes": "olist_order_items_dataset.csv",
    "Produits": "olist_products_dataset.csv",
    "Avis": "olist_order_reviews_dataset.csv"
}

for name, path in files.items():
    df = pd.read_csv(path)
    print(f"\n--- Structure de : {name} ---")
    print(f"Colonnes : {df.columns.tolist()}")
    print(f"Valeurs manquantes :\n{df.isnull().sum()[df.isnull().sum() > 0]}")
    print(df.head(3))