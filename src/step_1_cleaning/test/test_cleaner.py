import pytest
import pandas as pd
import numpy as np
import os

from step_1_cleaning import clean_data

def test_cleaner_logic(tmp_path):
    # 1. SETUP : On se déplace dans un dossier temporaire isolé
    os.chdir(tmp_path)

    # Cas à tester : Poids lourd sans catégorie -> doit devenir "heavy_volume"
    products_data = {
        'product_id': ['P1', 'P2', np.nan],
        'product_category_name': [None, 'tech', 'art'],
        'product_weight_g': [6000, 100, 100]
    }
    
    # Cas à tester : Review Score manquant -> doit devenir la médiane
    reviews_data = {
        'review_id': ['R1', 'R2', 'R3'],
        'review_score': [1, 5, np.nan] 
    }

    # Fichier items minimaliste
    items_data = {'order_id': ['O1'], 'product_id': ['P1']}

    pd.DataFrame(products_data).to_csv("olist_products_dataset.csv", index=False)
    pd.DataFrame(reviews_data).to_csv("olist_order_reviews_dataset.csv", index=False)
    pd.DataFrame(items_data).to_csv("olist_order_items_dataset.csv", index=False)

    clean_data()

    # 4. ASSERT : Vérification des résultats
    df_prod = pd.read_csv("clean_products.csv")
    df_rev = pd.read_csv("clean_reviews.csv")

    # Vérif suppression ID manquant
    assert len(df_prod) == 2 
    
    # Vérif imputation "heavy_volume"
    row_p1 = df_prod[df_prod['product_id'] == 'P1'].iloc[0]
    assert row_p1['product_category_name'] == 'heavy_volume'

    # Vérif imputation médiane (Médiane de 1, 5 est 3.0)
    row_nan_score = df_rev[df_rev['review_id'] == 'R3'].iloc[0]
    assert row_nan_score['review_score'] == 3.0