# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
import joblib
import asyncio

from database.db_down import gather_players_country
from data_processing.model.model_utils import preprocess_data

def train_and_save_model(model_path: str = 'model/model.pkl') -> None:
    """
    Trains a RandomForestClassifier model on the data from the players_country table and saves it to the specified path.
    """
    # Gather the data from the players in the players_country table
    df_players_country = gather_players_country()

    # Gather the player data from esea
    player_ids = df_players_country['player_id'].to_list()
    player_names = df_players_country['player_name'].to_list()

    # Do the benelux check for all players
    df = asyncio.run(check_all_players(player_ids, player_names))
    
    # Prepare the training data
    df_filtered = df.merge(df_players_country[['player_id', 'country']], on='player_id', how='left')
    df_filtered = preprocess_data(df_filtered.drop(columns=['bnlx_country']))
    
    # Prepare the data for training
    data = df_filtered.drop(columns=['player_id', 'player_name'])
    data = data.rename(columns={'country': 'target'})
    
    # Separate features and target variable
    X = data.drop(columns=['target'])
    y = data['target']

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize and train the model
    rfr = RandomForestClassifier(random_state=42)

    param_grid = {
        'n_estimators': [100, 200, 300],
        'max_depth': [None, 10, 20, 30],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4],
        'bootstrap': [True, False],
        'max_features': ['sqrt', 'log2']
    }

    grid_search = RandomizedSearchCV(
        estimator=rfr,
        param_distributions=param_grid,
        n_iter=100,
        cv=2,
        verbose=2,
        random_state=42,
        n_jobs=-1
    )

    grid_search.fit(X_train, y_train)

    # Get the best parameters from the grid search
    print("Best parameters found: ", grid_search.best_params_, '\n')

    model = grid_search.best_estimator_

    # Do a test prediction to check the model's performance
    y_pred = model.predict(X_test)

    print("Accuracy:", accuracy_score(y_test, y_pred))
    print("Precision:", precision_score(y_test, y_pred))
    print("Recall:", recall_score(y_test, y_pred))
    print("F1 Score:", f1_score(y_test, y_pred))
    print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred), '\n')
        
    # Save the trained model
    joblib.dump(model, model_path)
    
    print(f"Model saved to {model_path}")
    
if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    # Train the model on the players_country database
    from database.db_down import gather_players_country
    
    train_and_save_model()
    
    
    