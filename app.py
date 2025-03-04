import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit.components.v1 as components
from streamlit_plotly_events import plotly_events
from streamlit_option_menu import option_menu
from views import delays

# --- Configuration de la page en mode large ---
st.set_page_config(layout="wide")
 
with st.sidebar:
    selected = option_menu(
        "Navigation",
        ["Réseau Aéroportuaire", "Analyse des Retards"],
        icons=['diagram-3', 'clock-history'],
        menu_icon="airplane",
        default_index=0,
    )

if selected == "Réseau Aéroportuaire":
    # --- CSS pour réduire le padding et masquer la scrollbar ---
    st.markdown(
        """
        <style>

        ::-webkit-scrollbar {
            display: none;
        }
        html {
          -ms-overflow-style: none;  /* IE et Edge */
          scrollbar-width: none;  /* Firefox */
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # --- Authentification BigQuery ---
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = bigquery.Client(credentials=credentials)

    # --- Fonctions de cache pour les requêtes ---
    @st.cache_data(show_spinner=False)
    def get_years(_client):
        query_years = """
        SELECT DISTINCT flight_year
        FROM `flightops-analytics.dbt_jawadmecheri.airport_network`
        ORDER BY flight_year
        """
        return _client.query(query_years).to_dataframe()

    @st.cache_data(show_spinner=False)
    def get_edges(year, _client):
        query_edges = f"""
        SELECT *
        FROM `flightops-analytics.dbt_jawadmecheri.airport_network`
        WHERE flight_year = {year}
        """
        return _client.query(query_edges).to_dataframe()

    @st.cache_data(show_spinner=False)
    def get_nodes(year, _client):
        query_nodes = f"""
        SELECT a.flight_year,
               a.airport_key,
               a.flight_volume,
               a.cluster_class,
               a.betweenness_centrality,
               a.degree_centrality,
               a.avg_ground_time,
               d.latitude_deg,
               d.longitude_deg,
               d.name,
               d.home_link
        FROM `flightops-analytics.dbt_jawadmecheri.Agg_Airport_Network_Performance` a
        JOIN `flightops-analytics.dbt_jawadmecheri.dim_airport` d
          ON a.airport_key = d.airport_key
        WHERE a.flight_year = {year}
        """
        return _client.query(query_nodes).to_dataframe()

    @st.cache_data(show_spinner=False)
    def get_degree(airport_key, year, _client):
        query_degree = f"""
        SELECT
           (
             (SELECT COUNT(*) 
              FROM `flightops-analytics.dbt_jawadmecheri.airport_network`
              WHERE flight_year = {year} AND source_airport = '{airport_key}')
             +
             (SELECT COUNT(*) 
              FROM `flightops-analytics.dbt_jawadmecheri.airport_network`
              WHERE flight_year = {year} AND target_airport = '{airport_key}')
           ) AS degree
        """
        df = _client.query(query_degree).to_dataframe()
        if df.empty:
            return 0
        else:
            return df['degree'].iloc[0]

    @st.cache_data(show_spinner=False)
    def get_all_nodes(_client, years):
        frames = []
        for y in years:
            frames.append(get_nodes(y, _client))
        return pd.concat(frames, ignore_index=True)

    # --- Filtrage en haut de la page (sans barre latérale) ---
    df_years = get_years(client)
    years = sorted(df_years['flight_year'].tolist())

    # Organiser les filtres sur deux colonnes : à gauche l'aéroport, à droite l'année.
    col_airport, col_year = st.columns(2)
    with col_year:
        year_selected = st.selectbox("Année", years)
    # Récupérer les données pour l'année sélectionnée
    df_edges = get_edges(year_selected, client)
    df_nodes = get_nodes(year_selected, client)
    with col_airport:
        airport_names = sorted([x for x in df_nodes['name'].unique() if x is not None])
        selected_airport_name = st.selectbox("Aéroport", airport_names)
    selected_airport = df_nodes[df_nodes['name'] == selected_airport_name]['airport_key'].iloc[0]

    # --- Mise en forme des données ---
    def cluster_to_color(cluster):
        palette = {
            '0': [31, 120, 180],   # Bleu
            '1': [51, 160, 44],    # Vert
            '2': [227, 26, 28],    # Rouge
            '3': [255, 127, 0],    # Orange
            '4': [106, 61, 154],   # Violet
            '5': [177, 89, 40]     # Brun
        }
        return palette.get(str(cluster), [128, 128, 128])

    df_nodes['color'] = df_nodes['cluster_class'].apply(cluster_to_color)

    # Couleur des barres : du vert pour un temps court, au rouge pour un temps long.
    min_time = df_nodes['avg_ground_time'].min()
    max_time = df_nodes['avg_ground_time'].max()

    def compute_bar_color(time):
        norm = (time - min_time) / (max_time - min_time) if max_time > min_time else 0
        r = int(255 * norm)
        g = int(255 * (1 - norm))
        b = 0
        return [r, g, b, 180]

    df_nodes["bar_color"] = df_nodes["avg_ground_time"].apply(compute_bar_color)

    # Fusionner les coordonnées dans df_edges depuis df_nodes
    df_coords = df_nodes[['airport_key', 'latitude_deg', 'longitude_deg']].rename(
        columns={'airport_key': 'airport_key', 'latitude_deg': 'lat', 'longitude_deg': 'lon'}
    )
    df_edges = pd.merge(
        df_edges,
        df_coords.rename(columns={'airport_key': 'source_airport', 'lat': 'source_latitude', 'lon': 'source_longitude'}),
        on='source_airport',
        how='left'
    )
    df_edges = pd.merge(
        df_edges,
        df_coords.rename(columns={'airport_key': 'target_airport', 'lat': 'target_latitude', 'lon': 'target_longitude'}),
        on='target_airport',
        how='left'
    )

    # Récupération des données pour l'aéroport sélectionné
    selected_data = df_nodes[df_nodes['airport_key'] == selected_airport].iloc[0]

    # Affichage du titre avec le nom de l'aéroport
    st.title(selected_data['name'])

    # Affichage du lien si la valeur home_link est présente
    if pd.notnull(selected_data['home_link']) and selected_data['home_link'] != "":
        st.markdown(f"[Accédez au site]({selected_data['home_link']})", unsafe_allow_html=True)


    # --- Couches Pydeck ---
    # Filtrer les arêtes pour l'aéroport sélectionné
    df_edges_selected = df_edges[
        (df_edges['source_airport'] == selected_airport) |
        (df_edges['target_airport'] == selected_airport)
    ]

    # Couche pour toutes les liaisons (faible opacité)
    arc_layer_initial = pdk.Layer(
        "ArcLayer",
        data=df_edges,
        get_source_position=["source_longitude", "source_latitude"],
        get_target_position=["target_longitude", "target_latitude"],
        get_source_color=[39, 164, 183, 5],
        get_target_color=[39, 164, 183, 5],
        auto_highlight=True,
        width_scale=0.0005,
        get_width="weight",
        width_min_pixels=2,
        parameters={"depthTest": False},
    )

    # Couche pour les aéroports
    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=df_nodes,
        get_position=["longitude_deg", "latitude_deg"],
        get_fill_color="[color[0], color[1], color[2], 150]",
        auto_highlight=True,
        highlight_color=[200, 200, 200, 50],
        get_radius="degree_centrality",
        radius_scale=1,
        radius_min_pixels=2,
        pickable=True,
        parameters={"depthTest": False}
    )

    # Couche pour les liaisons de l'aéroport sélectionné
    arc_layer_selected = pdk.Layer(
        "ArcLayer",
        data=df_edges_selected,
        get_source_position=["source_longitude", "source_latitude"],
        get_target_position=["target_longitude", "target_latitude"],
        get_source_color=[255, 100, 100, 200],
        get_target_color=[255, 100, 100, 200],
        auto_highlight=True,
        width_scale=0.001,
        get_width="weight",
        width_min_pixels=2,
        parameters={"depthTest": False}
    )

    # Configuration de la vue initiale
    if not df_nodes.empty:
        view_state = pdk.ViewState(
            latitude=df_nodes['latitude_deg'].mean(),
            longitude=df_nodes['longitude_deg'].mean(),
            zoom=4,
            pitch=30,
        )
    else:
        view_state = pdk.ViewState(latitude=37.76, longitude=-122.4, zoom=4, pitch=30)

    # Assemblage du Deck
    deck = pdk.Deck(
        layers=[scatter_layer, arc_layer_initial, arc_layer_selected],
        initial_view_state=view_state,
        tooltip={
            "html": (
                "<div style='font-family: Roboto, sans-serif; font-size: 13px; "
                "box-shadow: 0px 2px 4px rgba(0,0,0,0.2);'>"
                    "<b>Aéroport :</b> {name}<br/>"
                    "<b>Groupe d'Aéroports :</b> {cluster_class}<br/>"
                    "<b>Degree Centrality:</b> {degree_centrality}<br/>"
                    "<b>Rôle de Carrefour :</b> {betweenness_centrality}<br/>"
                    "<b>Temps d'Attente au Sol (min) :</b> {avg_ground_time}<br/>"
                "</div>"
            )
        }



    )

    # --- Mise en page : KPI à gauche, Carte à droite ---
    col_map, col_kpi = st.columns([3, 1])
    with col_kpi:
        st.header("Indicateurs Clés")
        selected_rows = df_nodes[df_nodes['airport_key'] == selected_airport]
        if not selected_rows.empty:
            selected_data = selected_rows.iloc[0]
        else:
            selected_data = None

        if selected_data is not None:
            degree_direct = get_degree(selected_airport, year_selected, client)
            st.metric("Volume de Vols", f"{int(selected_data['flight_volume'])} vols")
            
            # Affichage du KPI avec icône info pour "Rôle de Carrefour"
            col_metric, col_info = st.columns([3, 0.3])
            with col_metric:
                st.metric("Rôle de Carrefour", f"{selected_data['betweenness_centrality']:.1f}")
            with col_info:
                st.markdown(
                    "<span title='Le Rôle de Carrefour mesure l’importance de l’aéroport comme point de passage essentiel dans le réseau, c’est-à-dire combien il aide à relier différentes destinations.'>🛈</span>",
                    unsafe_allow_html=True
                )
            # KPI Connexions Directes avec info
            col_metric, col_info = st.columns([3, 0.3])
            with col_metric:
                st.metric("Connexions Directes", f"{degree_direct}")
            with col_info:
                st.markdown(
                    "<span title='Les Connexions Directes représentent le nombre total de liaisons directes (vols) que l’aéroport entretient avec d’autres aéroports.'>🛈</span>",
                    unsafe_allow_html=True
                )
            # KPI Temps d'Attente au Sol avec info
            col_metric, col_info = st.columns([3, 0.3])
            with col_metric:
                st.metric("Temps d'Attente au Sol (min)", f"{int(selected_data['avg_ground_time'])} min")
            with col_info:
                st.markdown(
                    "<span title='Le Temps d’Attente au Sol correspond à la durée moyenne (taxi in et taxi out) que les avions passent au sol avant le décollage ou après l’atterrissage.'>🛈</span>",
                    unsafe_allow_html=True
                )
        else:
            st.write("Aucun aéroport sélectionné.")
    with col_map:
        # Utilisation de deck.to_html pour générer le code HTML complet de la carte
        html_map = deck.to_html(as_string=True)
        # CSS pour la légende (injectée dans la balise head)
        legend_css = """
        <style>
        .legend {
            position: absolute;
            bottom: 10px;
            left: 10px;
            background-color: rgba(255, 255, 255, 0.5);
            padding: 8px 12px;
            border-radius: 8px;
            font-family: 'Roboto', sans-serif;
            font-size: 11px;
            color: #333;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            line-height: 1.2;
            z-index: 1000;
        }
        .legend h4 {
            margin: 0 0 6px;
            font-size: 13px;
            font-weight: 600;
        }
        .legend p {
            margin: 2px 0;
        }
        .legend span {
            font-size: 14px;
            vertical-align: middle;
        }
        </style>
        """

        # Code HTML de la légende à injecter (placé juste après <body>)
        legend_html = """
        <div class="legend">
            <p><span style="color: rgb(31, 120, 180);">■</span> Groupe 0</p>
            <p><span style="color: rgb(51, 160, 44);">■</span> Groupe 1</p>
            <p><span style="color: rgb(227, 26, 28);">■</span> Groupe 2</p>
            <p><span style="color: rgb(255, 127, 0);">■</span> Groupe 3</p>
            <p><span style="color: rgb(106, 61, 154);">■</span> Groupe 4</p>
            <p><span style="color: rgb(177, 89, 40);">■</span> Groupe 5</p>
            <p><strong>Épaisseur des arcs :</strong> Nombre de vols par année sur cette route</p>
            <p>
                <strong>Taille des nœuds :</strong> Degree Centrality
                <span title="Le degree centrality représente non seulement le nombre de connexions directes (le degré du nœud) mais aussi d'autres mesures de connectivité qui reflètent l'importance globale de l'aéroport dans le réseau.">🛈</span>
            </p>
        </div>
        """
        # Injection du CSS et de la légende dans le HTML généré
        modified_html = html_map.replace("<head>", "<head>" + legend_css)
        modified_html = modified_html.replace("<body>", "<body>" + legend_html)
        # Affichage du HTML modifié dans un composant Streamlit
        components.html(modified_html, height=600, scrolling=False)

    # --- Analyse Temporelle des Indicateurs (normalisée) ---
    st.header("Analyse Temporelle des Indicateurs")
    df_all_nodes = get_all_nodes(client, years)
    df_time = df_all_nodes[df_all_nodes['airport_key'] == selected_airport].sort_values('flight_year').copy()

    # Préparer les valeurs brutes
    df_time['Volume de Vols'] = df_time['flight_volume'].astype(int)
    df_time["Temps d'Attente (min)"] = df_time['avg_ground_time'].round(0).astype(int)

    # Normalisation pour comparer l'évolution
    def normalize(series):
        if series.max() == series.min():
            return series - series.min()
        return (series - series.min()) / (series.max() - series.min())

    df_time['Volume_norm'] = normalize(df_time['Volume de Vols'])
    df_time['Temps_norm'] = normalize(df_time["Temps d'Attente (min)"])

    # Transformation en format long
    df_melt_norm = df_time.melt(
        id_vars='flight_year',
        value_vars=['Volume_norm', 'Temps_norm'],
        var_name='Indicateur',
        value_name='Valeur Normalisée'
    )

    # Renommer pour affichage
    indicator_map = {
        'Volume_norm': "Volume de Vols",
        'Temps_norm': "Temps d'Attente (min)"
    }
    df_melt_norm['Indicateur'] = df_melt_norm['Indicateur'].map(indicator_map)

    # Calcul de la valeur brute correspondant à chaque indicateur et année
    df_melt_norm['Valeur Brute'] = df_melt_norm.apply(
        lambda row: df_time.loc[df_time['flight_year'] == row['flight_year'], 'Volume de Vols'].values[0]
        if row['Indicateur'] == "Volume de Vols"
        else df_time.loc[df_time['flight_year'] == row['flight_year'], "Temps d'Attente (min)"].values[0],
        axis=1
    )

    # Création du graphique avec custom_data pour le hover
    fig_time_norm = px.line(
        df_melt_norm,
        x='flight_year',
        y='Valeur Normalisée',
        color='Indicateur',
        markers=True,
        title="Évolution normalisée des Indicateurs",
        custom_data=['Valeur Brute', 'Indicateur'],
        color_discrete_map={
            "Volume de Vols": "#6699CC",
            "Temps d'Attente (min)": "#d44242"
        }
    )

    for trace in fig_time_norm.data:
        trace.hovertemplate = (
            "Année: %{x}<br>"
            "%{customdata[1]} : %{customdata[0]}<extra></extra>"
        )

    fig_time_norm.update_yaxes(showticklabels=False, title_text='')
    st.plotly_chart(fig_time_norm)

    # --- Section : Analyse Comparative et Diagnostic ---
    col_comp, col_diag = st.columns([2, 1])
    with col_comp:
        st.header("Analyse Comparative")
        fig_scatter = px.scatter(
            df_nodes,
            x='flight_volume',
            y='avg_ground_time',
            color='cluster_class',
            hover_data=['name', 'betweenness_centrality'],
            title="Temps d'Attente au Sol vs Volume de Vols",
            labels={
                'flight_volume': "Volume de Vols",
                'avg_ground_time': "Temps d'Attente (min)",
                'cluster_class': "Groupe d'Aéroports",
                'betweenness_centrality': "Rôle de Carrefour"
            }
        )
        if selected_data is not None:
            fig_scatter.add_scatter(
                x=[selected_data['flight_volume']],
                y=[selected_data['avg_ground_time']],
                mode='markers',
                marker=dict(size=15, color='red'),
                name='Aéroport sélectionné'
            )
        st.plotly_chart(fig_scatter)

    with col_diag:
        st.header("Diagnostic et Recommandations")
        volume_threshold = df_nodes['flight_volume'].quantile(0.75)
        time_threshold = df_nodes['avg_ground_time'].quantile(0.75)
        if selected_data is not None:
            volume_is_high = selected_data['flight_volume'] >= volume_threshold
            time_is_high = selected_data['avg_ground_time'] >= time_threshold
            description_volume = "un trafic élevé" if volume_is_high else "un trafic modéré"
            description_temps = "un temps d'attente long" if time_is_high else "un temps d'attente court"
            st.write(f"**Diagnostic :** Cet aéroport présente {description_volume} et {description_temps}.")
            if volume_is_high and time_is_high:
                st.warning(
                    "Investissement recommandé : L'aéroport a un trafic important ET un temps d'attente élevé. "
                    "Investir dans les infrastructures pourrait améliorer la fluidité."
                )
            elif (not volume_is_high) and time_is_high:
                st.info(
                    "Réforme opérationnelle recommandée : Malgré un trafic modéré, le temps d'attente est élevé. "
                    "Une réorganisation des opérations est conseillée."
                )
            elif volume_is_high and (not time_is_high):
                st.success(
                    "Haute efficacité : L'aéroport gère un trafic élevé avec un temps d'attente raisonnable, "
                    "indiquant une bonne performance."
                )
            else:
                st.success(
                    "Performance équilibrée : L'aéroport présente un trafic modéré et un temps d'attente court, "
                    "ce qui est satisfaisant."
                )
        else:
            st.write("Aucun aéroport sélectionné.")

if selected == "Analyse des Retards":
    delays.afficher_dashboard()