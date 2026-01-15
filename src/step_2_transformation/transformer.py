import pandas as pd
import numpy as np

def log_stats(df, step_name):
    print(f"\n--- [LOG] Étape: {step_name} ---")
    print(f"Colonnes actuelles : {df.columns.tolist()}")
    missing = df.isnull().sum()
    print(f"Valeurs manquantes :\n{missing[missing > 0] if missing.sum() > 0 else 'Aucune'}")
    print("-" * 30)

def transform_data():
    # Chargement des données nettoyées par le Produit N°1
    items = pd.read_csv("clean_items.csv")
    products = pd.read_csv("clean_products.csv")

    log_stats(items, "Items - Avant Transformation")
    log_stats(products, "products - Avant Transformation")
    

    # 1. Calcul complexe sur les produits : Volume en cm3
    # On gère les NaNs potentiels sur les dimensions avant calcul
    products['product_volume_cm3'] = (
        products['product_length_cm'] * products['product_height_cm'] * products['product_width_cm']
    ).fillna(0)

    # 2. Feature Engineering sur les ventes : Ratio de Fret
    # Pourquoi ? Si le fret > 50% du prix, le produit est peu rentable
    items['freight_ratio'] = items['freight_value'] / items['price']
    items['freight_ratio'] = items['freight_ratio'].replace([np.inf, -np.inf], 0).fillna(0)

    # 3. Drop de colonnes (Nettoyage de structure)
    # On retire les colonnes techniques dont on n'a plus besoin pour la DB finale
    products_reduced = products.drop([
        'product_name_lenght', 
        'product_description_lenght', 
        'product_photos_qty'
    ], axis=1)

    items_reduced = items.drop([
        'shipping_limit_date'
    ], axis=1)

    # Sauvegarde
    items_reduced.to_csv("transformed_items.csv", index=False)
    products_reduced.to_csv("transformed_products.csv", index=False)
    print("Étape 2 terminée : Transformations et calculs complexes effectués.")

    log_stats(items, "Items - Après Transformation")
    log_stats(products, "products - Après Transformation")
    

if __name__ == "__main__":
    transform_data()