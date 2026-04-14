import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import xgboost as xgb
import matplotlib.pyplot as plt
import seaborn as sns
from xgboost import plot_tree
from sklearn.model_selection import train_test_split, cross_val_score, KFold, cross_val_predict
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
import numpy as np


#Configurazione del database
DB_CONFIG = {
    'dbname': 'nuovo_db',
    'user': 'postgres',
    'password': '1947934',
    'host': 'localhost',
    'port': '5432'
}

def create_db_uri(db_config):
    return f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

def fetch_data(db_config):
    db_uri = create_db_uri(db_config)
    engine = create_engine(db_uri)
    query = """
    SELECT m.sample_id, m.measure_id, m.measure_value, mt.name, mt.unita_misura
    FROM measurements m
    JOIN measurement_type mt ON m.measure_id = mt.measure_id
    WHERE mt.unita_misura = 'tpm' OR mt.name = 'vital_status'
    """
    return pd.read_sql(query, engine)

# Trasformazione dati
def transform_data(df):
    vital_status_df = df[df['name'] == 'vital_status'][['sample_id', 'measure_value']]
    vital_status_df = vital_status_df.rename(columns={'measure_value': 'vital_status'}).set_index('sample_id')
    
    genes_df = df[df['unita_misura'] == 'tpm'].pivot_table(
        index='sample_id', columns='name', values='measure_value', aggfunc='first'
    )
    
    df_pivot = genes_df.join(vital_status_df, on='sample_id')
    return df_pivot



#Pipeline principale
if __name__ == "__main__":
    #Recupera e trasforma i dati
    data = fetch_data(DB_CONFIG)
    df = transform_data(data)

    #Rimuove colonne con troppi NaN (es. >40%)
    threshold = 0.4
    nan_ratio = df.isna().sum() / len(df)
    cols_to_drop = nan_ratio[nan_ratio > threshold].index
    df.drop(columns=cols_to_drop, inplace=True)
    print(f"Colonne eliminate: {len(cols_to_drop)}")

    #Preprocessing dei dati
    df.fillna(0, inplace=True)  # Sostituisce NaN rimanenti con 0
    df['vital_status'] = df['vital_status'].replace({'Dead': 0, 'Alive': 1})
    df = df[df['vital_status'] != 'Not Reported']

    #Separazione feature e target
    X = df.drop(columns=['vital_status']).apply(pd.to_numeric)
    y = df['vital_status'].astype(int)

    #Standardizzazione
    scaler = StandardScaler()
    X = pd.DataFrame(scaler.fit_transform(X), columns=X.columns)

    #Divisione train-test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, shuffle=True, random_state=42)

    #Bilanciamento con SMOTE
    smote = SMOTE(random_state=42)
    X_train, y_train = smote.fit_resample(X_train, y_train)

    #Calcolo scale_pos_weight per XGBoost
    scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()
    print(f"Scale_pos_weight: {scale_pos_weight:.4f}")

    #Definizione del modello XGBoost con parametri specificati
    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=10,
        learning_rate=0.3,
        subsample=0.7,
        colsample_bytree=0.5,
        gamma=0,
        min_child_weight=3,
        scale_pos_weight=scale_pos_weight,
        eval_metric=['auc', 'logloss', 'error'],
        random_state=42,
        n_jobs=2
    )

    # Funzione per la confusion matrix
    """
def plot_confusion_matrix(y_true, y_pred, labels):
    cm = confusion_matrix(y_true, y_pred)
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=labels, yticklabels=labels)
    plt.xlabel('Predicted')
    plt.ylabel('True')
    plt.title('Confusion Matrix')
    plt.show()
    """

    #Addestramento del modello
    model.fit(X_train, y_train)

    #Cross-validation (solo su training set)
    k_folds = KFold(n_splits=5)
    scores = cross_val_score(model, X_train, y_train, cv=k_folds, scoring='accuracy')
    print(f"Accuracy media con cross-validation (training set): {scores.mean():.4f}")

    y_pred_cv = cross_val_predict(model, X_train, y_train, cv=k_folds)
    print(f"Accuracy cross-validation (training set): {accuracy_score(y_train, y_pred_cv):.4f}")
    print("Confusion Matrix (training set):")
    print(confusion_matrix(y_train, y_pred_cv))
    print("Classification Report (training set):")
    print(classification_report(y_train, y_pred_cv, target_names=['Dead', 'Alive']))

    #Valutazione finale su test set
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy finale sul test set: {accuracy:.4f}")

    #plot_confusion_matrix(y_test, y_pred, labels=['Dead', 'Alive'])

    #Salvataggio dell’albero XGBoost
    plt.figure(figsize=(20, 10))
    plot_tree(model, num_trees=0, rankdir='LR')
    plt.savefig("xgboost_tree_highres11.png", dpi=300)
    plt.close()
    print("Immagine dell'albero salvata come 'xgboost_tree_highres11.png'")
