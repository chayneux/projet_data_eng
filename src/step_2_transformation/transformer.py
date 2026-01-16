import pandas as pd
import numpy as np
import os

def log_stats(df, step_name):
    print(f"\n--- [LOG] Étape: {step_name} ---")
    print(f"Colonnes actuelles : {df.columns.tolist()}")
    missing = df.isnull().sum()
    print(f"Valeurs manquantes :\n{missing[missing > 0] if missing.sum() > 0 else 'Aucune'}")
    print("-" * 30)

def transform_data():
    # 1. Gestion du chemin (Local vs S3)
    bucket_name = os.getenv('S3_BUCKET')
    
    # On vérifie si on est en mode test local
    if os.path.exists("clean_items.csv"):
        input_base = "."
        output_base = "."
        print("Mode local détecté (Tests).")
    else:
        if not bucket_name:
            raise ValueError("S3_BUCKET non défini et fichiers 'clean' absents.")
        input_base = f"s3://{bucket_name}/clean"
        output_base = f"s3://{bucket_name}/transformed"
        print(f"Mode Cloud détecté. Lecture depuis : {input_base}")

    # 2. Chargement des données (Nettoyées par l'étape 1)
    items = pd.read_csv(f"{input_base}/clean_items.csv")
    products = pd.read_csv(f"{input_base}/clean_products.csv")

    log_stats(items, "Items - Avant Transformation")
    log_stats(products, "products - Avant Transformation")

    # 3. Calcul complexe sur les produits : Volume en cm3
    products['product_volume_cm3'] = (
        products['product_length_cm'] * products['product_height_cm'] * products['product_width_cm']
    ).fillna(0)

    # 4. Feature Engineering sur les ventes : Ratio de Fret
    items['freight_ratio'] = items['freight_value'] / items['price']
    items['freight_ratio'] = items['freight_ratio'].replace([np.inf, -np.inf], 0).fillna(0)

    # 5. Drop de colonnes (Nettoyage de structure)
    products_reduced = products.drop([
        'product_name_lenght', 
        'product_description_lenght', 
        'product_photos_qty'
    ], axis=1, errors='ignore') # errors='ignore' évite de planter si déjà supprimé

    items_reduced = items.drop([
        'shipping_limit_date'
    ], axis=1, errors='ignore')

    # 6. Sauvegarde
    items_reduced.to_csv(f"{output_base}/transformed_items.csv", index=False)
    products_reduced.to_csv(f"{output_base}/transformed_products.csv", index=False)
    
    print(f"Étape 2 terminée : Fichiers sauvegardés dans {output_base}")

    log_stats(items_reduced, "Items - Après Transformation")
    log_stats(products_reduced, "products - Après Transformation")

if __name__ == "__main__":
    transform_data()