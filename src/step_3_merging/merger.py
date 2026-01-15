import pandas as pd

def log_stats(df, step_name):
    print(f"\n--- [LOG] Étape: {step_name} ---")
    print(f"Colonnes actuelles : {df.columns.tolist()}")
    missing = df.isnull().sum()
    print(f"Valeurs manquantes :\n{missing[missing > 0] if missing.sum() > 0 else 'Aucune'}")
    print("-" * 30)


def merge_and_finalize():
    # Chargement des fichiers transformés
    items = pd.read_csv("transformed_items.csv")
    products = pd.read_csv("transformed_products.csv")
    reviews = pd.read_csv("clean_reviews.csv")

    # 1. Fusion (Join) Items + Products
    # On fait un 'inner' pour n'avoir que des ventes avec des infos produits valides
    df_merged = pd.merge(items, products, on='product_id', how='inner')

    # 2. Fusion avec les Avis (Reviews) sur l'order_id
    # Attention : un order_id peut avoir plusieurs items, on duplique le score
    final_df = pd.merge(df_merged, reviews[['order_id', 'review_score']], on='order_id', how='left')

    # 3. Calcul de la colonne "Alert_High_Risk"
    # Un produit est risqué si Score < 2 ET Ratio Fret > 0.3
    final_df['is_high_risk'] = ((final_df['review_score'] < 2) & (final_df['freight_ratio'] > 0.3)).astype(int)

    # Sauvegarde finale avant DB
    final_df.to_csv("final_dataset_to_db.csv", index=False)
    print(f"Étape 3 terminée : Fusion réussie. {len(final_df)} lignes prêtes pour PostgreSQL.")

    log_stats(final_df, "CSV FINAL")


if __name__ == "__main__":
    merge_and_finalize()