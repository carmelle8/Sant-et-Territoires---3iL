"""
Dashboard Streamlit — Santé & Territoires
Projet Big Data 3iL · Pipeline Medallion

Visuels répondant au défi data.gouv.fr :
  1. Carte des déserts médicaux par département
  2. Écarts offre/demande classés (Top sous-dotés / sur-dotés)
  3. Analyse par profil territorial (clustering K-Means)
  4. Indicateurs environnementaux de santé
  5. Tableau de bord avec KPIs et filtres

Lancement : streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import boto3
import io
from botocore.exceptions import ClientError

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Santé & Territoires",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

MINIO_ENDPOINT = "http://localhost:9000"
BUCKET = "datalakesante"

# ─────────────────────────────────────────────────────────────
# CONNEXION MINIO
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

@st.cache_data(ttl=300)
def load_parquet(key: str) -> pd.DataFrame | None:
    try:
        obj = get_s3().get_object(Bucket=BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except ClientError:
        return None

@st.cache_data(ttl=300)
def list_bronze_files() -> list:
    try:
        r = get_s3().list_objects_v2(Bucket=BUCKET, Prefix="bronze/")
        return [o["Key"] for o in r.get("Contents", [])]
    except ClientError:
        return []

# ─────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🏥 Santé & Territoires")
    st.caption("Défi Open Data University · 3iL Limoges")
    st.divider()

    st.subheader("⚙️ Paramètres")
    seuil_desert = st.slider(
        "Seuil désert médical (%)",
        min_value=-80, max_value=-5, value=-20, step=5,
        help="Un territoire est considéré comme sous-doté si son écart est inférieur à ce seuil"
    )
    top_n = st.slider("Nombre de territoires affichés", 10, 50, 20, 5)

    st.divider()
    st.subheader("🏗️ Architecture Medallion")
    st.markdown("""
    ```
    CSV (data.gouv.fr)
         ↓ Kafka
    🟤 Bronze (brut)
         ↓ nettoyage
    ⚪ Silver (Parquet)
         ↓ XGBoost
    🟡 Gold (diagnostic)
         ↓
    📊 Dashboard
    ```
    """)

    st.divider()
    if st.button("🔄 Rafraîchir les données"):
        st.cache_data.clear()
        st.rerun()

# ─────────────────────────────────────────────────────────────
# CHARGEMENT
# ─────────────────────────────────────────────────────────────
df_gold   = load_parquet("gold/final_diagnostic_results.parquet")
df_silver = load_parquet("silver/final_health_territory.parquet")
bronze_files = list_bronze_files()

# ─────────────────────────────────────────────────────────────
# ÉTAT DU PIPELINE — toujours visible
# ─────────────────────────────────────────────────────────────
st.title("🏥 Diagnostic Territorial de Santé")
st.caption("Identifier les déserts médicaux et les inégalités d'accès aux soins en France")

col_b, col_s, col_g = st.columns(3)
col_b.metric("🟤 Bronze", f"{len(bronze_files)} fichiers", help="Données brutes dans MinIO")
col_s.metric("⚪ Silver", "✅ Prêt" if df_silver is not None else "⏳ En attente")
col_g.metric("🟡 Gold",   "✅ Prêt" if df_gold is not None else "⏳ En attente")

if df_gold is None and df_silver is None and not bronze_files:
    st.error("❌ Aucune donnée dans MinIO. Lancez le DAG Airflow sur http://localhost:8081")
    with st.expander("🔧 Diagnostic de connexion"):
        try:
            buckets = get_s3().list_buckets()
            st.success(f"✅ MinIO accessible — Buckets : {[b['Name'] for b in buckets['Buckets']]}")
        except Exception as e:
            st.error(f"❌ MinIO inaccessible : {e}")
    st.stop()

st.divider()

# ─────────────────────────────────────────────────────────────
# CAS GOLD DISPONIBLE — dashboard complet
# ─────────────────────────────────────────────────────────────
if df_gold is not None:

    # Nommage intelligent de la colonne territoire
    col_territoire = next(
        (c for c in ["departement", "dep_nom", "libelle", "nom_commune", "territoire"]
         if c in df_gold.columns),
        df_gold.columns[0]
    )

    # Filtres
    col_f1, col_f2 = st.columns(2)

    if "reg_nom" in df_gold.columns:
        regions = ["Toutes les régions"] + sorted(df_gold["reg_nom"].dropna().unique())
        region_sel = col_f1.selectbox("🗺️ Région", regions)
        df_view = df_gold[df_gold["reg_nom"] == region_sel] if region_sel != "Toutes les régions" else df_gold
    else:
        df_view = df_gold

    if "cluster" in df_view.columns:
        cluster_labels = {0: "0 — Urbain dense", 1: "1 — Intermédiaire", 2: "2 — Rural isolé"}
        clusters = ["Tous les profils"] + [cluster_labels.get(i, str(i)) for i in sorted(df_view["cluster"].dropna().unique())]
        cluster_sel = col_f2.selectbox("🏘️ Profil territorial", clusters)
        if cluster_sel != "Tous les profils":
            num = int(cluster_sel.split("—")[0].strip())
            df_view = df_view[df_view["cluster"] == num]

    # ── KPIs ──────────────────────────────────────────────────
    deserts   = df_view[df_view["ecart_pourcentage"] < seuil_desert]
    bien_dote = df_view[df_view["ecart_pourcentage"] > 0]
    ecart_med = df_view["ecart_pourcentage"].median()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Territoires analysés", len(df_view))
    k2.metric("🔴 Zones sous-dotées", len(deserts),
              delta=f"< {seuil_desert}%", delta_color="inverse")
    k3.metric("🟢 Zones bien dotées", len(bien_dote))
    k4.metric("Écart médian", f"{ecart_med:.1f}%")

    st.divider()

    # ── ONGLETS ───────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Classement des déserts",
        "🗺️ Carte territoriale",
        "🔬 Analyse par profil",
        "📈 Indicateurs environnementaux",
        "📋 Données brutes"
    ])

    # ── TAB 1 : CLASSEMENT ────────────────────────────────────
    with tab1:
        st.subheader("Classement des territoires par écart offre/demande")
        st.caption("Un écart négatif indique un territoire sous-doté (désert médical potentiel)")

        col_g1, col_g2 = st.columns(2)

        # Top N sous-dotés
        with col_g1:
            st.markdown("#### 🔴 Zones les plus sous-dotées")
            df_sous = df_view.nsmallest(top_n, "ecart_pourcentage")
            df_sous["couleur"] = df_sous["ecart_pourcentage"].apply(
                lambda x: "#e74c3c" if x < seuil_desert else "#e67e22"
            )
            fig1 = px.bar(
                df_sous,
                x="ecart_pourcentage",
                y=col_territoire,
                orientation="h",
                color="ecart_pourcentage",
                color_continuous_scale=["#c0392b", "#e67e22"],
                range_color=[df_sous["ecart_pourcentage"].min(), 0],
                labels={"ecart_pourcentage": "Écart (%)", col_territoire: ""},
                height=max(400, top_n * 22)
            )
            fig1.add_vline(x=seuil_desert, line_dash="dash", line_color="black",
                           annotation_text=f"Seuil désert ({seuil_desert}%)")
            fig1.update_layout(coloraxis_showscale=False, margin=dict(l=0))
            st.plotly_chart(fig1, use_container_width=True)

        # Top N bien dotés
        with col_g2:
            st.markdown("#### 🟢 Zones les mieux dotées")
            df_bien = df_view.nlargest(top_n, "ecart_pourcentage")
            fig2 = px.bar(
                df_bien,
                x="ecart_pourcentage",
                y=col_territoire,
                orientation="h",
                color="ecart_pourcentage",
                color_continuous_scale=["#27ae60", "#2ecc71"],
                labels={"ecart_pourcentage": "Écart (%)", col_territoire: ""},
                height=max(400, top_n * 22)
            )
            fig2.update_layout(coloraxis_showscale=False, margin=dict(l=0))
            st.plotly_chart(fig2, use_container_width=True)

        # Distribution globale
        st.markdown("#### Distribution globale des écarts")
        fig_hist = px.histogram(
            df_view,
            x="ecart_pourcentage",
            nbins=40,
            color_discrete_sequence=["#3498db"],
            labels={"ecart_pourcentage": "Écart offre/demande (%)"},
            title="Distribution des écarts — tous territoires"
        )
        fig_hist.add_vline(x=0, line_color="gray", line_width=2, annotation_text="Équilibre")
        fig_hist.add_vline(x=seuil_desert, line_dash="dash", line_color="red",
                           annotation_text=f"Seuil désert ({seuil_desert}%)")
        fig_hist.add_vrect(x0=df_view["ecart_pourcentage"].min(), x1=seuil_desert,
                           fillcolor="red", opacity=0.05, annotation_text="Zone critique")
        st.plotly_chart(fig_hist, use_container_width=True)

    # ── TAB 2 : CARTE ─────────────────────────────────────────
    with tab2:
        st.subheader("Carte des inégalités d'accès aux soins")

        lat_col = next((c for c in df_view.columns if "lat" in c.lower()), None)
        lon_col = next((c for c in df_view.columns if "lon" in c.lower() or "lng" in c.lower()), None)

        if lat_col and lon_col:
            df_map = df_view.dropna(subset=[lat_col, lon_col, "ecart_pourcentage"])
            df_map["taille"] = df_map["ecart_pourcentage"].abs().clip(1, 100)
            df_map["statut"] = df_map["ecart_pourcentage"].apply(
                lambda x: "🔴 Sous-doté" if x < seuil_desert
                else ("🟢 Bien doté" if x > 0 else "⚪ Neutre")
            )

            fig_map = px.scatter_mapbox(
                df_map,
                lat=lat_col, lon=lon_col,
                color="ecart_pourcentage",
                color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                color_continuous_midpoint=0,
                size="taille",
                hover_name=col_territoire,
                hover_data={"ecart_pourcentage": ":.1f", lat_col: False, lon_col: False, "taille": False},
                zoom=5,
                center={"lat": 46.6, "lon": 2.3},
                mapbox_style="carto-positron",
                height=580,
                title="Carte de l'accès aux soins — Rouge = désert médical"
            )
            fig_map.update_coloraxes(colorbar_title="Écart (%)")
            st.plotly_chart(fig_map, use_container_width=True)

        else:
            # Carte alternative : barres par département
            st.info("ℹ️ Colonnes latitude/longitude absentes — affichage alternatif par département.")

            if "departement" in df_view.columns:
                df_dept = (df_view.groupby("departement")["ecart_pourcentage"]
                           .mean().reset_index()
                           .sort_values("ecart_pourcentage"))

                df_dept["couleur"] = df_dept["ecart_pourcentage"].apply(
                    lambda x: "Sous-doté 🔴" if x < seuil_desert
                    else ("Bien doté 🟢" if x > 0 else "Neutre ⚪")
                )
                fig_dept = px.bar(
                    df_dept,
                    x="departement",
                    y="ecart_pourcentage",
                    color="couleur",
                    color_discrete_map={
                        "Sous-doté 🔴": "#e74c3c",
                        "Bien doté 🟢": "#2ecc71",
                        "Neutre ⚪": "#95a5a6"
                    },
                    title="Écart moyen offre/demande par département",
                    labels={"ecart_pourcentage": "Écart moyen (%)", "departement": "Département"},
                    height=450
                )
                fig_dept.add_hline(y=seuil_desert, line_dash="dash", line_color="red",
                                   annotation_text=f"Seuil désert ({seuil_desert}%)")
                st.plotly_chart(fig_dept, use_container_width=True)

    # ── TAB 3 : PROFILS ───────────────────────────────────────
    with tab3:
        st.subheader("Analyse par profil territorial (Clustering K-Means)")
        st.caption("Segmentation des territoires en 3 profils homogènes pour une comparaison équitable")

        if "cluster" in df_view.columns:
            cluster_map = {0: "Urbain dense", 1: "Intermédiaire", 2: "Rural isolé"}
            df_view = df_view.copy()
            df_view["profil"] = df_view["cluster"].map(cluster_map).fillna("Inconnu")
            colors = {"Urbain dense": "#3498db", "Intermédiaire": "#e67e22", "Rural isolé": "#27ae60"}

            c1, c2 = st.columns(2)

            # Répartition
            fig_pie = px.pie(
                df_view, names="profil",
                title="Répartition des territoires par profil",
                color="profil", color_discrete_map=colors,
                hole=0.35
            )
            c1.plotly_chart(fig_pie, use_container_width=True)

            # Boxplot des écarts
            fig_box = px.box(
                df_view, x="profil", y="ecart_pourcentage",
                color="profil", color_discrete_map=colors,
                title="Distribution des écarts offre/demande par profil",
                labels={"ecart_pourcentage": "Écart (%)", "profil": ""}
            )
            fig_box.add_hline(y=seuil_desert, line_dash="dash", line_color="red",
                              annotation_text="Seuil désert")
            fig_box.add_hline(y=0, line_color="gray", line_width=1)
            c2.plotly_chart(fig_box, use_container_width=True)

            # Stats par profil
            st.markdown("#### Statistiques par profil")
            stats = (df_view.groupby("profil")["ecart_pourcentage"]
                     .agg(["count", "mean", "median", "std"])
                     .round(2)
                     .reset_index())
            stats.columns = ["Profil", "Nb territoires", "Écart moyen (%)", "Écart médian (%)", "Écart-type"]
            stats["Zones critiques"] = df_view[df_view["ecart_pourcentage"] < seuil_desert].groupby("profil").size().reindex(stats["Profil"]).fillna(0).astype(int).values
            st.dataframe(stats, use_container_width=True)

            # Si population disponible
            pop_col = next((c for c in df_view.columns if "pop" in c.lower()), None)
            if pop_col:
                df_view[pop_col] = pd.to_numeric(df_view[pop_col], errors="coerce")
                fig_scatter = px.scatter(
                    df_view.dropna(subset=[pop_col, "ecart_pourcentage"]),
                    x=pop_col,
                    y="ecart_pourcentage",
                    color="profil",
                    color_discrete_map=colors,
                    hover_name=col_territoire,
                    title="Population vs Écart offre/demande",
                    labels={pop_col: "Population", "ecart_pourcentage": "Écart (%)"},
                    log_x=True, height=400
                )
                fig_scatter.add_hline(y=seuil_desert, line_dash="dash", line_color="red")
                fig_scatter.add_hline(y=0, line_color="gray")
                st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.info("La colonne 'cluster' sera disponible après le run Gold complet.")

    # ── TAB 4 : ENVIRONNEMENT ─────────────────────────────────
    with tab4:
        st.subheader("Indicateurs environnementaux et sociaux de santé")
        st.caption("Croisement entre l'accès aux soins et les déterminants sociaux/environnementaux")

        env_cols = [c for c in df_view.columns
                    if any(k in c.lower() for k in
                           ["pollution", "air", "indice", "precari", "social", "revenu",
                            "chomage", "pauvrete", "environn", "garches", "score"])]

        if env_cols:
            env_sel = st.multiselect("Indicateurs à afficher", env_cols, default=env_cols[:2])

            for col in env_sel:
                df_env = df_view[[col_territoire, col, "ecart_pourcentage"]].dropna()
                df_env[col] = pd.to_numeric(df_env[col], errors="coerce")

                fig_env = px.scatter(
                    df_env,
                    x=col,
                    y="ecart_pourcentage",
                    hover_name=col_territoire,
                    trendline="ols",
                    color="ecart_pourcentage",
                    color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                    color_continuous_midpoint=0,
                    title=f"Corrélation : {col} vs Accès aux soins",
                    labels={col: col.replace("_", " ").title(),
                            "ecart_pourcentage": "Écart offre/demande (%)"},
                    height=380
                )
                fig_env.add_hline(y=seuil_desert, line_dash="dash", line_color="red",
                                  annotation_text="Seuil désert")
                st.plotly_chart(fig_env, use_container_width=True)

        else:
            st.info("""
            ℹ️ Les indicateurs environnementaux seront disponibles après enrichissement du Silver
            avec les données de la base santé Garches (vf-base-sante-garches.csv).

            **Indicateurs prévus :**
            - Qualité de l'air (particules fines PM2.5, PM10)
            - Indice de précarité sociale
            - Taux de chômage local
            - Exposition aux risques environnementaux
            """)

            # Corrélation disponible avec population si pas d'indicateurs env.
            pop_col = next((c for c in df_view.columns if "pop" in c.lower()), None)
            nb_etab = "nb_etablissements" if "nb_etablissements" in df_view.columns else None

            if pop_col and nb_etab:
                st.markdown("#### Disponible : corrélation population × établissements de santé")
                df_corr = df_view[[col_territoire, pop_col, nb_etab, "ecart_pourcentage"]].dropna()
                df_corr[pop_col] = pd.to_numeric(df_corr[pop_col], errors="coerce")
                df_corr[nb_etab] = pd.to_numeric(df_corr[nb_etab], errors="coerce")

                fig_corr = px.scatter(
                    df_corr,
                    x=pop_col, y=nb_etab,
                    color="ecart_pourcentage",
                    color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                    color_continuous_midpoint=0,
                    hover_name=col_territoire,
                    trendline="ols",
                    title="Population vs Nombre d'établissements de santé",
                    labels={pop_col: "Population totale", nb_etab: "Nb établissements"},
                    log_x=True, height=420
                )
                st.plotly_chart(fig_corr, use_container_width=True)

    # ── TAB 5 : DONNÉES BRUTES ────────────────────────────────
    with tab5:
        st.subheader("Données Gold complètes")

        search = st.text_input("🔎 Rechercher", placeholder="Nom du territoire, département...")
        if search:
            mask = df_view.apply(lambda col: col.astype(str).str.contains(search, case=False)).any(axis=1)
            df_display = df_view[mask]
        else:
            df_display = df_view

        st.caption(f"{len(df_display)} lignes affichées")
        st.dataframe(
            df_display.sort_values("ecart_pourcentage"),
            use_container_width=True, height=430
        )

        csv_bytes = df_display.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Exporter en CSV",
            csv_bytes,
            f"diagnostic_sante_territoires_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
            "text/csv"
        )

# ─────────────────────────────────────────────────────────────
# CAS SILVER SEULEMENT
# ─────────────────────────────────────────────────────────────
elif df_silver is not None:
    st.warning("⚠️ La couche Gold n'est pas encore prête. Données Silver en cours d'analyse.")
    st.metric("Lignes Silver disponibles", len(df_silver))
    st.dataframe(df_silver.head(50), use_container_width=True)
    st.info("👉 Lancez le DAG complet dans Airflow pour générer les prédictions Gold.")

# ─────────────────────────────────────────────────────────────
# CAS BRONZE SEULEMENT
# ─────────────────────────────────────────────────────────────
elif bronze_files:
    st.warning("⚠️ Seule la couche Bronze est disponible. Le pipeline Silver/Gold n'a pas encore tourné.")
    st.markdown("**Fichiers ingérés dans MinIO Bronze :**")
    for f in bronze_files:
        st.markdown(f"- `{f}`")
    st.info("👉 Dans Airflow (http://localhost:8081), vérifiez les logs des tâches `transformation_silver` et `analyse_ml_gold`.")