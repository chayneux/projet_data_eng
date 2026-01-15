import pytest
import pandas as pd
import os

from step_3_merging import merge_and_finalize

def test_merger_logic(tmp_path):
    os.chdir(tmp_path)

    # 1. MOCK : Cas critique "High Risk"
    # Score faible (1) + Ratio élevé (0.4) -> Doit être High Risk
    
    items_data = {
        'order_id': ['O1'], 'product_id': ['P1'],
        'freight_ratio': [0.4] 
    }
    
    products_data = {
        'product_id': ['P1'],
        'product_category_name': ['auto']
    }
    
    reviews_data = {
        'order_id': ['O1'],
        'review_score': [1] # Mauvaise note
    }

    pd.DataFrame(items_data).to_csv("transformed_items.csv", index=False)
    pd.DataFrame(products_data).to_csv("transformed_products.csv", index=False)
    pd.DataFrame(reviews_data).to_csv("clean_reviews.csv", index=False)

    # 2. ACTION
    merge_and_finalize()

    # 3. ASSERT
    df_final = pd.read_csv("final_dataset_to_db.csv")

    # Vérification de la jointure
    assert len(df_final) == 1
    
    # Vérification de la logique métier (Risk Score)
    # Score(1) < 2 AND Ratio(0.4) > 0.3 ====> HIGH RISK (1)
    assert df_final.iloc[0]['is_high_risk'] == 1