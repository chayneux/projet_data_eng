import pandas as pd
import numpy as np
import os

def log_stats(df, step_name):
    print(f"\n--- [LOG] Étape: {step_name} ---")
    print(f"Colonnes actuelles : {df.columns.tolist()}")
    missing = df.isnull().sum()
    print(f"Valeurs manquantes :\n{missing[missing > 0] if missing.sum() > 0 else 'Aucune'}")
    print("-" * 30)

def clean_data():
    bucket_name = os.getenv('S3_BUCKET')
    
    # Si on est en test local (fichiers présents), on utilise le chemin relatif
    # Sinon, on utilise le chemin S3
    if os.path.exists("olist_products_dataset.csv"):
        input_base = "."
        output_base = "."
        print("Mode local détecté pour les tests.")
    else:
        if not bucket_name:
            raise ValueError("S3_BUCKET non défini et fichiers locaux absents.")
        input_base = f"s3://{bucket_name}/raw"
        output_base = f"s3://{bucket_name}/clean"
        print(f"Mode Cloud détecté. Bucket: {bucket_name}")

    # Chargement
    items = pd.read_csv(f"{input_base}/olist_order_items_dataset.csv")
    products = pd.read_csv(f"{input_base}/olist_products_dataset.csv")
    reviews = pd.read_csv(f"{input_base}/olist_order_reviews_dataset.csv")

    log_stats(items, "Items - Avant Nettoyage")
    log_stats(products, "products - Avant Nettoyage")
    log_stats(reviews, "reviews - Avant Nettoyage")

    # --- NETTOYAGE PRODUITS ---
    products.dropna(subset=['product_id'], inplace=True)
    
    def impute_category(row):
        if pd.isna(row['product_category_name']):
            return "heavy_volume" if row['product_weight_g'] > 5000 else "light_volume"
        return row['product_category_name']
    
    products['product_category_name'] = products.apply(impute_category, axis=1)

    # --- NETTOYAGE VENTES ---
    items.dropna(subset=['order_id', 'product_id'], inplace=True)

    # --- NETTOYAGE AVIS ---
    median_score = reviews['review_score'].median()
    reviews['review_score'] = reviews['review_score'].fillna(median_score)

    # 3. Sauvegarde sur S3 (Dossier 'clean' pour la sortie)
    items.to_csv(f"{output_base_path}/clean_items.csv", index=False)
    products.to_csv(f"{output_base_path}/clean_products.csv", index=False)
    reviews.to_csv(f"{output_base_path}/clean_reviews.csv", index=False)
    
    print(f"Étape 1 terminée : Fichiers sauvegardés dans {output_base_path}")

    log_stats(items, "Items - Après Nettoyage")
    log_stats(products, "products - Après Nettoyage")
    log_stats(reviews, "reviews - Après Nettoyage")

if __name__ == "__main__":
    clean_data()