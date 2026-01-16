import pandas as pd
import os
from sqlalchemy import create_engine

def log_stats(df, step_name):
    print(f"\n--- [LOG] Étape: {step_name} ---")
    print(f"Lignes : {len(df)} | Colonnes : {len(df.columns)}")
    print(f"Colonnes : {df.columns.tolist()}")
    print("-" * 30)

def merge_and_finalize():
    # 1. Gestion du chemin (Local vs S3)
    bucket_name = os.getenv('S3_BUCKET')
    db_url = os.getenv('DATABASE_URL') # On récupère le secret Kubernetes
    
    if os.path.exists("transformed_items.csv"):
        input_transfo = "."
        input_clean = "."
        print("Mode local détecté.")
    else:
        if not bucket_name:
            raise ValueError("S3_BUCKET non défini.")
        input_transfo = f"s3://{bucket_name}/transformed"
        input_clean = f"s3://{bucket_name}/clean"
        print(f"Mode Cloud détecté. Lecture S3...")

    # 2. Chargement des fichiers
    items = pd.read_csv(f"{input_transfo}/transformed_items.csv")
    products = pd.read_csv(f"{input_transfo}/transformed_products.csv")
    reviews = pd.read_csv(f"{input_clean}/clean_reviews.csv")

    # 3. Fusion (Join)
    df_merged = pd.merge(items, products, on='product_id', how='inner')
    final_df = pd.merge(df_merged, reviews[['order_id', 'review_score']], on='order_id', how='left')

    # 4. Feature Engineering Final
    # Un produit est risqué si Score < 2 ET Ratio Fret > 0.3
    final_df['is_high_risk'] = ((final_df['review_score'] < 2) & (final_df['freight_ratio'] > 0.3)).astype(int)

    log_stats(final_df, "Dataset Final")

    # 5. Export vers PostgreSQL (RDS)
    if db_url:
        try:
            print("Tentative d'export vers RDS PostgreSQL...")
            engine = create_engine(db_url)
            # On crée la table 'olist_final_analytics' (remplace si elle existe)
            final_df.to_sql('olist_final_analytics', engine, if_exists='replace', index=False)
            print("Succès : Données insérées dans la base RDS !")
        except Exception as e:
            print(f"Erreur lors de l'insertion DB : {e}")
    else:
        # Sauvegarde CSV locale si pas de DB_URL (pour les tests)
        final_df.to_csv("final_dataset_to_db.csv", index=False)
        print("Avertissement : DATABASE_URL non trouvée. Sauvegarde en CSV local.")

if __name__ == "__main__":
    merge_and_finalize()