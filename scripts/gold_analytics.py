import pandas as pd
import boto3
import io
from sklearn.cluster import KMeans # type: ignore
from sklearn.preprocessing import StandardScaler # type: ignore
from xgboost import XGBRegressor # type: ignore

def run_ml_diagnostic():
    s3 = boto3.client('s3', 
                    endpoint_url='http://minio:9000',
                    aws_access_key_id='minioadmin', 
                    aws_secret_access_key='minioadmin')
    bucket = 'datalakesante'

    # 1. Lecture du Silver
    obj = s3.get_object(Bucket=bucket, Key='silver/final_health_territory.parquet')
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

    # 2. Clustering des Territoires (K-Means) 
    # On utilise les features de structure [cite: 87]
    features_clust = ['population_totale', 'nb_etablissements', 'tvs_par_habitant']
    scaler = StandardScaler() [cite: 88]
    df_scaled = scaler.fit_transform(df[features_clust])
    
    kmeans = KMeans(n_clusters=3, random_state=42) # 3 profils territoriaux [cite: 89, 92]
    df['cluster'] = kmeans.fit_predict(df_scaled) [cite: 94]

    # 3. Modélisation XGBoost 
    # Variables prédictives suggérées par le rapport [cite: 104]
    FEATURES = ['population_totale', 'nb_tvs', 'nb_etablissements', 'etablissements_par_habitant', 'cluster']
    TARGET = 'patients_uniques' [cite: 104]

    model = XGBRegressor(n_estimators=400, learning_rate=0.05, max_depth=4, random_state=42) [cite: 111]
    model.fit(df[FEATURES], df[TARGET])

    # 4. Calcul des écarts (Diagnostic final) [cite: 159, 160]
    df['patients_predits'] = model.predict(df[FEATURES])
    df['ecart_pourcentage'] = (df[TARGET] - df['patients_predits']) / df['patients_predits'] * 100 [cite: 160]

    # Sauvegarde Gold
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(Bucket=bucket, Key='gold/final_diagnostic_results.parquet', Body=buffer.getvalue())
    print("🏆 Couche Gold terminée : Clustering et Diagnostic XGBoost effectués.")
