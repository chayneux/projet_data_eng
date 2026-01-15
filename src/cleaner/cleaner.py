import pandas as pd
import numpy as np

def log_stats(df, step_name):
    print(f"\n--- [LOG] Étape: {step_name} ---")
    print(f"Colonnes actuelles : {df.columns.tolist()}")
    missing = df.isnull().sum()
    print(f"Valeurs manquantes :\n{missing[missing > 0] if missing.sum() > 0 else 'Aucune'}")
    print("-" * 30)

def clean_data():
    # Chargement
    items = pd.read_csv("olist_order_items_dataset.csv")
    products = pd.read_csv("olist_products_dataset.csv")
    reviews = pd.read_csv("olist_order_reviews_dataset.csv")

    log_stats(items, "Items - Avant Nettoyage")
    log_stats(products, "products - Avant Nettoyage")
    log_stats(reviews, "reviews - Avant Nettoyage")

    # --- NETTOYAGE PRODUITS ---
    # Attribut A : Suppression si ID manquant
    products.dropna(subset=['product_id'], inplace=True)
    
    # Attribut B : Harmonisation complexe (Catégorie basée sur le poids)
    def impute_category(row):
        if pd.isna(row['product_category_name']):
            return "heavy_volume" if row['product_weight_g'] > 5000 else "light_volume"
        return row['product_category_name']
    
    products['product_category_name'] = products.apply(impute_category, axis=1)

    # --- NETTOYAGE VENTES ---
    items.dropna(subset=['order_id', 'product_id'], inplace=True)

    # --- NETTOYAGE AVIS ---
    # Harmonisation complexe : Imputation du score par la médiane
    median_score = reviews['review_score'].median()
    reviews['review_score'] = reviews['review_score'].fillna(median_score)

    # Sauvegarde des fichiers "Clean" pour le produit suivant
    items.to_csv("clean_items.csv", index=False)
    products.to_csv("clean_products.csv", index=False)
    reviews.to_csv("clean_reviews.csv", index=False)
    print("Étape 1 terminée : Fichiers nettoyés et sauvegardés.")

    log_stats(items, "Items - Après Nettoyage")
    log_stats(products, "products - Après Nettoyage")
    log_stats(reviews, "reviews - Après Nettoyage")

if __name__ == "__main__":
    clean_data()