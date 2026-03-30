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

    # Nettoyage du nom de colonne (BOM unicode possible)
    df.columns = [c.strip().replace('\ufeff', '') for c in df.columns]

    print(f"✅ Silver chargé : {df.shape[0]} lignes, {df.shape[1]} colonnes")

    # 2. Suppression des lignes incomplètes
    features_clust = ['patients_uniques_integer', 'nb_etablissements', 'patients_medecin_traitant_integer']
    df = df.dropna(subset=features_clust)
    print(f"   Après nettoyage : {df.shape[0]} lignes")

    # 3. Clustering K-Means (3 profils territoriaux)
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df[features_clust])

    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    df['cluster'] = kmeans.fit_predict(df_scaled)
    print(f"✅ Clustering K-Means terminé : {df['cluster'].value_counts().to_dict()}")

    # 4. Modélisation XGBoost
    FEATURES = ['nb_etablissements', 'patients_medecin_traitant_integer', 'cluster']
    TARGET = 'patients_uniques_integer'

    df_model = df.dropna(subset=FEATURES + [TARGET])

    model = XGBRegressor(n_estimators=400,
    
    
     learning_rate=0.05, max_depth=4, random_state=42)
    model.fit(df_model[FEATURES], df_model[TARGET])
    print("✅ Modèle XGBoost entraîné")

    # 5. Calcul des écarts = indicateur de désert médical
    df['patients_predits'] = model.predict(df[FEATURES])
    df['ecart_pourcentage'] = (df[TARGET] - df['patients_predits']) / df['patients_predits'] * 100

    print(f"✅ Diagnostic calculé")
    print(f"   Territoires sous-dotés (écart < -20%) : {(df['ecart_pourcentage'] < -20).sum()}")
    print(f"   Territoires bien dotés  (écart > 20%)  : {(df['ecart_pourcentage'] > 20).sum()}")

    # 6. Sauvegarde Gold
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(Bucket=bucket, Key='gold/final_diagnostic_results.parquet', Body=buffer.getvalue())
    print("🏆 Couche Gold terminée : résultats sauvegardés dans MinIO !")
