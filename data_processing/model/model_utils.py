# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

_model = None

def preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the input data for prediction.
    
    Args:
        data (pd.DataFrame): The input data to be preprocessed.
    
    Returns:
        pd.DataFrame: The preprocessed data.
    """
    # Find columns with any None values and convert them using get_dummies()
    none_columns = data.columns[data.isnull().any()]
    data = pd.get_dummies(data, columns=none_columns, dummy_na=True)
    
    # Convert all columns with bool values to int
    bool_columns = data.select_dtypes(include=['bool']).columns
    data[bool_columns] = data[bool_columns].astype(int)
    
    # Convert all columns with country code string values to 1 or 0 depending on if the country code is in the benelux
    benelux_countries = {'nl', 'be', 'lu'}
    
    country_columns = [
        col for col in data.columns
        if data[col].dropna().astype(str).str.lower().str.fullmatch(r'[a-z]{2}').any()
    ]
    for col in country_columns:
        data[col] = data[col].apply(lambda x: 1 if str(x).lower() in benelux_countries else 0)
    
    return data

def load_model(model_path: str = 'model/model.pkl') -> RandomForestClassifier:
    global _model
    if _model is None:
        _model = joblib.load(model_path)
    return _model

def predict(data: pd.DataFrame):
    """
    Prediction function for a dataset
    
    Args:
        data (pd.DataFrame): The input data for prediction consisting of:
            - Features (X) excluding the target variable.

    Returns:
        numpy.ndarray: The predicted values for the input data.
    """
    model = load_model()
    
    pred = model.predict(data)
    
    return pred

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))