import pytest
import pandas as pd
import os
from transformer import transform_data

from step_2_transformation import transform_data

def test_transformer_logic(tmp_path):
    os.chdir(tmp_path)

    # 1. MOCK : Données "propres" en entrée
    # Produit : 10x10x10 = 1000 cm3
    products_data = {
        'product_id': ['P1'],
        'product_length_cm': [10], 'product_height_cm': [10], 'product_width_cm': [10],
        'product_name_lenght': [5], 'product_description_lenght': [5], 'product_photos_qty': [1] # Colonnes à supprimer
    }

    # Item : Prix 100, Fret 20 -> Ratio 0.2
    items_data = {
        'order_id': ['O1'], 'product_id': ['P1'],
        'price': [100.0], 'freight_value': [20.0],
        'shipping_limit_date': ['2023-01-01'] # Colonne à supprimer
    }

    pd.DataFrame(products_data).to_csv("clean_products.csv", index=False)
    pd.DataFrame(items_data).to_csv("clean_items.csv", index=False)

    # 2. ACTION
    transform_data()

    # 3. ASSERT
    df_trans_prod = pd.read_csv("transformed_products.csv")
    df_trans_items = pd.read_csv("transformed_items.csv")

    # Vérif Calcul Volume
    assert df_trans_prod.iloc[0]['product_volume_cm3'] == 1000.0

    # Vérif Drop de colonne
    assert 'product_photos_qty' not in df_trans_prod.columns

    # Vérif Ratio Fret
    assert df_trans_items.iloc[0]['freight_ratio'] == 0.2