# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import joblib
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier

_model = None

def preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the input data for prediction.
    
    Args:
        data (pd.DataFrame): The input data to be preprocessed. It should be the output dataframe from the process_player_country_details function.
    
    Returns:
        pd.DataFrame: The preprocessed data.
    """
    
    # Remove the 'player_id' and 'benelux_country' columns if they exist
    data_filtered = data.drop(columns=['bnlx_country'], errors='ignore')
        
    # Find columns with any None values and convert them using get_dummies()
    none_columns = data_filtered.columns[data_filtered.isnull().any()]
    
    # Ensure NaNs are actually np.nan
    data_filtered[none_columns] = data_filtered[none_columns].apply(lambda col: col.where(pd.notnull(col), np.nan))
    
    # Define full category set for each target column
    for col in none_columns:
        data_filtered[col] = pd.Categorical(data_filtered[col], categories=[True, False])
    
    data_dummies = pd.get_dummies(data_filtered, columns=list(none_columns), dummy_na=True)
    
    # Convert all columns with bool values to int
    bool_columns = data_dummies.select_dtypes(include=['bool']).columns
    data_dummies[bool_columns] = data_dummies[bool_columns].astype(int)
    
    # Convert all columns with country code string values to 1 or 0 depending on if the country code is in the benelux
    benelux_countries = {'nl', 'be', 'lu'}
    
    country_columns = [
        col for col in data_dummies.columns
        if data_dummies[col].dropna().astype(str).str.lower().str.fullmatch(r'[a-z]{2}').any()
    ]
    
    for col in country_columns:
        data_dummies[col] = data_dummies[col].apply(lambda x: 1 if str(x).lower() in benelux_countries else 0)
    
    return data_dummies

def predict(data: pd.DataFrame):
    """
    Prediction function for a dataset
    
    Args:
        data (pd.DataFrame): The input data to be predicted. It should be the output dataframe from the process_player_country_details function.

    Returns:
        numpy.ndarray: The predicted values for the input data. Will be the input df with an additional 'prediction' column. (0 or 1 for benelux)
    """
    model = load_model()
    
    data_copy = data.copy()
    
    df_processed = preprocess_data(data_copy)
    
    df_processed = df_processed.drop(columns=['player_id'], errors='ignore')  # Ensure 'player_id' is not in the processed data
    # Load or store the correct column order from training
    expected_columns = model.feature_names_in_  # Works with scikit-learn >=1.0

    # If the model was trained with a specific set of columns, ensure the processed data has the same columns
    if not set(expected_columns).issubset(df_processed.columns):
        raise ValueError("Processed data does not contain all expected columns from the model.")
    
    # Add missing columns with zeros, and ensure correct order
    df_processed = df_processed.reindex(columns=expected_columns, fill_value=0)
    
    pred = model.predict(df_processed)
    
    # Add the pred dataframe back to the original dataframe
    data_copy['prediction'] = pred
    df_predict = data_copy
    
    return df_predict

def load_model(model_path: str = 'data_processing/model/model.pkl') -> RandomForestClassifier:
    global _model
    if _model is None:
        _model = joblib.load(model_path)
    return _model

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))