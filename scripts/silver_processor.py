import sys
sys.path.append('/root/.local/lib/python3.8/site-packages')

import pandas as pd
import boto3
import io
import json
from datetime import datetime

def run_cleaning():
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )

    BUCKET = 'datalakesante'
    date_str = datetime.now().strftime("%Y-%m-%d")

    def get_df(filename):
        """Charge un fichier JSON depuis la couche Bronze de MinIO."""
        key = f"bronze/date={date_str}/{filename}"
        print(f"📂 Chargement : {key}")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            records = json.loads(obj['Body'].read().decode('utf-8'))
            df = pd.DataFrame(records)
            print(f"   → {len(df)} lignes, {len(df.columns)} colonnes")
            return df
        except Exception as e:
            print(f"   ❌ Erreur : {e}")
            return pd.DataFrame()

    print("🛠️  Début de la transformation Silver...")

    # Chargement des deux fichiers principaux
    df_patientele = get_df('patientele.csv')
    df_finess     = get_df('etalab-cs1100507-stock-20260107-0342.csv')

    if df_patientele.empty or df_finess.empty:
        raise ValueError("❌ Données manquantes dans le Bronze. Vérifiez que le Consumer a bien tourné.")

    print(f"\nColonnes patientele  : {df_patientele.columns.tolist()}")
    print(f"Colonnes finess      : {df_finess.columns.tolist()}")

    # ── Agrégation FINESS par département ──────────────────────
    # On cherche la colonne département dans le fichier FINESS
    dept_col = next(
        (c for c in df_finess.columns if 'dept' in c.lower() or 'dep' in c.lower()),
        None
    )
    finess_col = next(
        (c for c in df_finess.columns if 'finess' in c.lower()),
        df_finess.columns[0]
    )

    if dept_col:
        df_finess_grouped = df_finess.groupby(dept_col, as_index=False).agg(
            nb_etablissements=(finess_col, "count")
        ).rename(columns={dept_col: 'departement'})
    else:
        print("⚠️  Colonne département non trouvée dans FINESS — agrégation ignorée.")
        df_finess_grouped = pd.DataFrame(columns=['departement', 'nb_etablissements'])

    # ── Jointure patientele + FINESS ───────────────────────────
    # On cherche la colonne département dans la patientele
    dept_col_pat = next(
        (c for c in df_patientele.columns if 'dept' in c.lower() or 'dep' in c.lower()),
        None
    )

    if dept_col_pat and not df_finess_grouped.empty:
        df_patientele = df_patientele.rename(columns={dept_col_pat: 'departement'})
        df_merged = pd.merge(df_patientele, df_finess_grouped, on='departement', how='left')
        print(f"\n✅ Jointure réussie : {len(df_merged)} lignes")
    else:
        print("⚠️  Jointure impossible — utilisation de la patientele seule.")
        df_merged = df_patientele.copy()
        df_merged['nb_etablissements'] = 0

    # ── Feature Engineering (sans data leakage) ────────────────
    pop_col = next(
        (c for c in df_merged.columns if 'pop' in c.lower()),
        None
    )

    if pop_col and 'nb_etablissements' in df_merged.columns:
        df_merged[pop_col] = pd.to_numeric(df_merged[pop_col], errors='coerce')
        df_merged['nb_etablissements'] = pd.to_numeric(df_merged['nb_etablissements'], errors='coerce').fillna(0)
        df_merged['etablissements_par_habitant'] = (
            df_merged['nb_etablissements'] / df_merged[pop_col].replace(0, float('nan'))
        )
        df_merged = df_merged.rename(columns={pop_col: 'population_totale'})

    # ── Nettoyage final ────────────────────────────────────────
    df_merged = df_merged.drop_duplicates()
    avant = len(df_merged)
    df_merged = df_merged.dropna(how='all')
    print(f"🧹 Nettoyage : {avant} → {len(df_merged)} lignes")

    # ── Sauvegarde en Parquet dans Silver ──────────────────────
    buffer = io.BytesIO()
    df_merged.to_parquet(buffer, index=False, engine='pyarrow')
    s3.put_object(
        Bucket=BUCKET,
        Key='silver/final_health_territory.parquet',
        Body=buffer.getvalue()
    )
    print("✨ Couche Silver générée : silver/final_health_territory.parquet")

if __name__ == "__main__":
    run_cleaning()