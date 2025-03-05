import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date

# --- Configuration de la page ---
def afficher_dashboard():
  st.title("Dashboard Retards et Performances par Aéroport et Compagnie")

  # --- Authentification BigQuery ---
  credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
  client = bigquery.Client(credentials=credentials)

  # --- Récupération dynamique de la plage de dates disponibles ---
  @st.cache_data(show_spinner=False)
  def get_date_range():
      query = """
      SELECT MIN(FL_DATE) as min_date, MAX(FL_DATE) as max_date
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations`
      """
      df = client.query(query).to_dataframe()
      if not df.empty:
          min_date = pd.to_datetime(df["min_date"].iloc[0]).date()
          max_date = pd.to_datetime(df["max_date"].iloc[0]).date()
          return min_date, max_date
      else:
          return date(2018,1,1), date(2018,12,31)

  min_date, max_date = get_date_range()

  # --- Récupération dynamique des options de filtres ---
  @st.cache_data(show_spinner=False)
  def get_airport_options():
      query = """
      SELECT DISTINCT f.origin_airport_code, a.name AS airport_name
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      JOIN `flightops-analytics.dbt_jawadmecheri.dim_airport` a
        ON f.origin_airport_code = a.iata_code
      ORDER BY airport_name
      """
      df = client.query(query).to_dataframe()
      options = {"Tous": "Tous"}
      for _, row in df.iterrows():
          options[row["airport_name"]] = row["origin_airport_code"]
      return options

  @st.cache_data(show_spinner=False)
  def get_carrier_options():
      query = """
      SELECT DISTINCT carrier_code
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations`
      ORDER BY carrier_code
      """
      df = client.query(query).to_dataframe()
      options = ["Tous"] + sorted(df["carrier_code"].dropna().unique().tolist())
      return options

  @st.cache_data(show_spinner=False)
  def get_carrier_options_for_airport(selected_airport, start_date, end_date):
      query = f"""
      SELECT DISTINCT carrier_code
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations`
      WHERE FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND origin_airport_code = '{selected_airport}'
      ORDER BY carrier_code
      """
      df = client.query(query).to_dataframe()
      options = ["Tous"] + sorted(df["carrier_code"].dropna().unique().tolist())
      return options

  # Fonction pour obtenir la répartition par jour de la semaine
  @st.cache_data(show_spinner=False)
  def get_weekday_status(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        d.day_of_week as jour,
        SUM(CASE WHEN f.ARR_DELAY > 15 THEN 1 ELSE 0 END) as vols_retard,
        SUM(CASE WHEN f.ARR_DELAY <= 15 THEN 1 ELSE 0 END) as vols_a_temps
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      JOIN `flightops-analytics.dbt_jawadmecheri.dim_date` d
        ON f.date_key = d.date_key
      WHERE f.FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
      GROUP BY d.day_of_week
      ORDER BY d.day_of_week
      """
      return client.query(query).to_dataframe()

  # Nouvelle fonction pour la répartition à double couche (sunburst) par période de la journée
  @st.cache_data(show_spinner=False)
  def get_period_status_breakdown(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        t.period_of_day,
        CASE WHEN f.ARR_DELAY > 15 THEN 'En retard' ELSE 'À temps' END AS Statut,
        COUNT(*) as nb_vols
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      JOIN `flightops-analytics.dbt_jawadmecheri.dim_time` t
        ON f.departure_time_key = t.time_key
      WHERE f.FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
      GROUP BY t.period_of_day, Statut
      ORDER BY t.period_of_day, Statut
      """
      return client.query(query).to_dataframe()

  # --- Filtres dans la sidebar ---
  st.sidebar.header("Filtres")
  airport_options = get_airport_options()
  selected_airport_name = st.sidebar.selectbox("Aéroport de départ", options=list(airport_options.keys()), index=0)
  selected_airport = airport_options[selected_airport_name]

  date_range = st.sidebar.date_input(
    "Période", 
    value=[min_date, max_date], 
    min_value=min_date, 
    max_value=max_date
  )
  if isinstance(date_range, (list, tuple)):
      if len(date_range) == 2:
          start_date = date_range[0].strftime("%Y-%m-%d")
          end_date = date_range[1].strftime("%Y-%m-%d")
      elif len(date_range) == 1:
          start_date = date_range[0].strftime("%Y-%m-%d")
          end_date = start_date
  else:
      start_date = date_range.strftime("%Y-%m-%d")
      end_date = start_date


  if selected_airport != "Tous":
      carrier_options = get_carrier_options_for_airport(selected_airport, start_date, end_date)
  else:
      carrier_options = get_carrier_options()

  selected_carrier = st.sidebar.selectbox("Compagnie", options=carrier_options, index=0)

  # Bouton pour appliquer les filtres
  apply_filters = st.sidebar.button("Appliquer les filtres")

  def sql_filter(column, value):
      if value and value != "Tous":
          return f"{column} = '{value}'"
      else:
          return "1=1"

  airport_filter = sql_filter("f.origin_airport_code", selected_airport)
  carrier_filter = sql_filter("f.carrier_code", selected_carrier)

  # --- Fonctions de requête restantes ---

  @st.cache_data(show_spinner=False)
  def get_kpi_data(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        COUNT(*) AS total_flights,
        AVG(DEP_DELAY) AS avg_dep_delay,
        AVG(ARR_DELAY) AS avg_arr_delay,
        (SUM(CASE WHEN ARR_DELAY > 15 THEN 1 ELSE 0 END)/COUNT(*))*100 AS pct_delayed
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      WHERE FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
      """
      return client.query(query).to_dataframe()

  @st.cache_data(show_spinner=False)
  def get_daily_delay_distribution(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        FL_DATE as date,
        COUNT(*) as vols,
        AVG(DEP_DELAY) as retard_moyen_dep,
        AVG(ARR_DELAY) as retard_moyen_arr
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      WHERE FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
      GROUP BY FL_DATE
      ORDER BY FL_DATE
      """
      return client.query(query).to_dataframe()

  @st.cache_data(show_spinner=False)
  def get_top_destinations(start_date, end_date, selected_airport):
      query = f"""
      SELECT
        f.destination_airport_code,
        a.name as destination_name,
        COUNT(*) as vols,
        AVG(ARR_DELAY) as retard_moyen_arr
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      JOIN `flightops-analytics.dbt_jawadmecheri.dim_airport` a
        ON f.destination_airport_code = a.iata_code
      WHERE FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND f.origin_airport_code = '{selected_airport}'
      GROUP BY f.destination_airport_code, a.name
      ORDER BY retard_moyen_arr DESC
      LIMIT 10
      """
      return client.query(query).to_dataframe()

  @st.cache_data(show_spinner=False)
  def get_temporal_analysis(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        d.date_full as date,
        d.holiday_flag,
        d.holiday_name,
        d.day_of_week,
        COUNT(*) as vols,
        AVG(f.DEP_DELAY) as retard_moyen_dep,
        AVG(f.ARR_DELAY) as retard_moyen_arr
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      JOIN `flightops-analytics.dbt_jawadmecheri.dim_date` d
        ON f.date_key = d.date_key
      WHERE f.FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
      GROUP BY d.date_full, d.holiday_flag, d.holiday_name, d.day_of_week
      ORDER BY d.date_full
      """
      return client.query(query).to_dataframe()

  @st.cache_data(show_spinner=False)
  def get_hourly_delay_distribution(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        t.hour,
        COUNT(*) as vols,
        AVG(f.DEP_DELAY) as retard_moyen_dep,
        AVG(f.ARR_DELAY) as retard_moyen_arr
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      JOIN `flightops-analytics.dbt_jawadmecheri.dim_time` t
        ON f.departure_time_key = t.time_key
      WHERE f.FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
      GROUP BY t.hour
      ORDER BY t.hour
      """
      return client.query(query).to_dataframe()

  @st.cache_data(show_spinner=False)
  def get_delay_by_period(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        t.period_of_day,
        COUNT(*) as vols,
        AVG(f.DEP_DELAY) as retard_moyen_dep,
        AVG(f.ARR_DELAY) as retard_moyen_arr
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      JOIN `flightops-analytics.dbt_jawadmecheri.dim_time` t
        ON f.departure_time_key = t.time_key
      WHERE f.FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
      GROUP BY t.period_of_day
      """
      return client.query(query).to_dataframe()

  @st.cache_data(show_spinner=False)
  def get_total_delay_by_period(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        t.period_of_day,
        SUM(f.ARR_DELAY) as total_delay
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      JOIN `flightops-analytics.dbt_jawadmecheri.dim_time` t
        ON f.departure_time_key = t.time_key
      WHERE f.FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
      GROUP BY t.period_of_day
      """
      return client.query(query).to_dataframe()

  @st.cache_data(show_spinner=False)
  def get_delay_cause_distribution(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        SUM(CARRIER_DELAY) as total_delay_compagnie,
        SUM(WEATHER_DELAY) as total_delay_meteo,
        SUM(NAS_DELAY) as total_delay_NAS,
        SUM(SECURITY_DELAY) as total_delay_securite,
        SUM(LATE_AIRCRAFT_DELAY) as total_delay_avion_tardif
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      WHERE f.FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
      """
      return client.query(query).to_dataframe()

  @st.cache_data(show_spinner=False)
  def get_delay_cause_evolution(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        FL_DATE as date,
        COUNT(*) as vols,
        SUM(CARRIER_DELAY) as total_delay_compagnie,
        SUM(WEATHER_DELAY) as total_delay_meteo,
        SUM(NAS_DELAY) as total_delay_NAS,
        SUM(SECURITY_DELAY) as total_delay_securite,
        SUM(LATE_AIRCRAFT_DELAY) as total_delay_avion_tardif
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      WHERE f.FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
      GROUP BY FL_DATE
      ORDER BY FL_DATE
      """
      return client.query(query).to_dataframe()

  @st.cache_data(show_spinner=False)
  def get_delay_minutes_distribution(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        CAST(FLOOR(ARR_DELAY/5)*5 AS INT64) as tranche_retard,
        COUNT(*) as nb_vols
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      WHERE f.FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
        AND ARR_DELAY IS NOT NULL
        AND ARR_DELAY < 400
        AND ARR_DELAY > -100
      GROUP BY tranche_retard
      ORDER BY tranche_retard
      """
      return client.query(query).to_dataframe()

  @st.cache_data(show_spinner=False)
  def get_period_status_breakdown(start_date, end_date, airport_filter, carrier_filter):
      query = f"""
      SELECT
        t.period_of_day,
        CASE WHEN f.ARR_DELAY > 15 THEN 'En retard' ELSE 'À temps' END AS Statut,
        COUNT(*) as nb_vols
      FROM `flightops-analytics.dbt_jawadmecheri.fact_flight_operations` f
      JOIN `flightops-analytics.dbt_jawadmecheri.dim_time` t
        ON f.departure_time_key = t.time_key
      WHERE f.FL_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND {airport_filter}
        AND {carrier_filter}
      GROUP BY t.period_of_day, Statut
      ORDER BY t.period_of_day, Statut
      """
      return client.query(query).to_dataframe()
  loading_message = st.empty()  # Crée un placeholder

  # --- Exécution des requêtes si les filtres sont appliqués ---
  if apply_filters:
      loading_message.info("Chargement des données...")
      kpi_df = get_kpi_data(start_date, end_date, airport_filter, carrier_filter)
      daily_df = get_daily_delay_distribution(start_date, end_date, airport_filter, carrier_filter)
      temporal_df = get_temporal_analysis(start_date, end_date, airport_filter, carrier_filter)
      hourly_df = get_hourly_delay_distribution(start_date, end_date, airport_filter, carrier_filter)
      delay_period_df = get_delay_by_period(start_date, end_date, airport_filter, carrier_filter)
      total_delay_period_df = get_total_delay_by_period(start_date, end_date, airport_filter, carrier_filter)
      cause_dist_df = get_delay_cause_distribution(start_date, end_date, airport_filter, carrier_filter)
      cause_evol_df = get_delay_cause_evolution(start_date, end_date, airport_filter, carrier_filter)
      top_dest_df = pd.DataFrame()
      if selected_airport != "Tous":
          top_dest_df = get_top_destinations(start_date, end_date, selected_airport)
      delay_minutes_df = get_delay_minutes_distribution(start_date, end_date, airport_filter, carrier_filter)
      weekday_df = get_weekday_status(start_date, end_date, airport_filter, carrier_filter)
      period_status_df = get_period_status_breakdown(start_date, end_date, airport_filter, carrier_filter)

      # ================================================
      # 1. Indicateurs principaux (KPIs)
      # ================================================
      st.subheader("Indicateurs principaux")
      if not kpi_df.empty:
          total_flights = int(kpi_df["total_flights"].iloc[0])
          avg_dep_delay = kpi_df["avg_dep_delay"].iloc[0]
          avg_arr_delay = kpi_df["avg_arr_delay"].iloc[0]
          pct_delayed = kpi_df["pct_delayed"].iloc[0]
          col1, col2, col3, col4 = st.columns(4)
          col1.metric("Nombre total de vols", f"{total_flights:,}")

          dep_label = "Avance moyenne au départ" if avg_dep_delay < 0 else "Retard moyen au départ"
          arr_label = "Avance moyenne à l'arrivée" if avg_arr_delay < 0 else "Retard moyen à l'arrivée"

          col2.metric(dep_label, f"{abs(avg_dep_delay):.0f} min")
          col3.metric(arr_label, f"{abs(avg_arr_delay):.0f} min")
          col4.metric("Pourcentage de vols en retard", f"{pct_delayed:.0f} %")

      else:
          st.warning("Aucune donnée KPI trouvée.")

      # ================================================
      # 2. Répartition par période de la journée 
      # ================================================
      st.subheader("Répartition par période de la journée")
      cols = st.columns(2)
      with cols[0]:
        if not period_status_df.empty:
            couleurs = {"Matin": "#636EFA", "Après-midi": "#EF553B", "Soir": "#00CC96", "Nuit": "#AB63FA"}

            fig_sunburst = px.sunburst(
                period_status_df,
                path=["period_of_day", "Statut"],
                values="nb_vols",
                color="period_of_day",
                color_discrete_map=couleurs,
                title="Répartition des vols par période et statut"
            )
            st.plotly_chart(fig_sunburst, use_container_width=True)
        else:
            st.warning("Aucune donnée pour la répartition par période.")

      with cols[1]:
          if not total_delay_period_df.empty:
              total_delay_period_df["total_delay"] = total_delay_period_df["total_delay"].apply(lambda x: x if x > 0 else 0).round(0)
              fig_donut_delais = px.pie(
                  total_delay_period_df,
                  names="period_of_day",
                  values="total_delay",
                  hole=0.4,
                  color="period_of_day",
                  color_discrete_map=couleurs
              )
              fig_donut_delais.update_layout(title="Retards totaux par période")
              st.plotly_chart(fig_donut_delais, use_container_width=True)
          else:
              st.warning("Aucune donnée sur les retards par période.")

      # ================================================
      # 3. Histogramme de la distribution des retards 
      # ================================================
      st.subheader("Distribution des retards")
      if not delay_minutes_df.empty:
          fig_hist = px.bar(
              delay_minutes_df,
              x="tranche_retard",
              y="nb_vols",
              labels={"tranche_retard": "Retard (minutes)", "nb_vols": "Nombre de vols"},
              title="Histogramme des retards (limité à 400 min)"
          )
          st.plotly_chart(fig_hist, use_container_width=True)
      else:
          st.warning("Aucune donnée pour l'histogramme des retards.")

      # ================================================
      # 4. Top destinations (si un aéroport est sélectionné)
      # ================================================
      if selected_airport != "Tous":
          st.subheader(f"Top destinations par retard moyen - {selected_airport}")
          if not top_dest_df.empty:
              cols = st.columns(2)
              with cols[0]:
                st.dataframe(top_dest_df)
                fig_top_dest = px.bar(
                    top_dest_df,
                    x="destination_name",
                    y="retard_moyen_arr",
                    labels={"destination_name": "Destination", "retard_moyen_arr": "Retard moyen (min)"},
                    title=""
                )
              with cols[1]:
                st.plotly_chart(fig_top_dest, use_container_width=True)
          else:
              st.info("Aucune donnée pour les destinations.")

      # ================================================
      # 5. Analyse temporelle quotidienne (courbe lissée avec jours fériés)
      # ================================================
      st.subheader("Évolution quotidienne des retards")
      if not temporal_df.empty:
          temporal_df['date'] = pd.to_datetime(temporal_df['date'])
          temporal_df = temporal_df.sort_values("date")
          temporal_df['moyenne_glissante'] = temporal_df['retard_moyen_arr'].rolling(window=7, min_periods=1).mean()
          overall_mean = temporal_df["retard_moyen_arr"].mean()
          fig_temp = px.line(
              temporal_df,
              x="date",
              y="retard_moyen_arr",
              labels={"retard_moyen_arr": "Retard moyen (min)", "date": "Date"},
              title="Tendance quotidienne des retards"
          )
          fig_temp.add_scatter(
              x=temporal_df["date"],
              y=temporal_df["moyenne_glissante"],
              mode="lines",
              line=dict(dash="dash", width=3, color="blue"),
              name="Tendance (7 jours)"
          )
          fig_temp.add_hline(
              y=overall_mean,
              line_dash="dash",
              line_color="gray",
              annotation_text="Moyenne générale",
              annotation_position="bottom right"
          )
          holidays_df = temporal_df[temporal_df["holiday_flag"] == True]
          if not holidays_df.empty:
              fig_temp.add_scatter(
                  x=holidays_df["date"],
                  y=holidays_df["retard_moyen_arr"],
                  mode="markers",
                  marker=dict(color="red", size=8, symbol="x"),
                  name="Jours fériés",
                  text=holidays_df["holiday_name"],
                  hovertemplate="Jour férié : %{text}<br>Date : %{x}<br>Retard : %{y:.0f} min<extra></extra>"
              )
              for idx, row in holidays_df.iterrows():
                  fig_temp.add_vline(
                      x=row["date"],
                      line_dash="dot",
                      line_color="red",
                      opacity=0.5
                  )
          st.plotly_chart(fig_temp, use_container_width=True)
      else:
          st.warning("Aucune donnée temporelle disponible.")

      # ================================================
      # 6. Répartition des vols par jour de la semaine (barres empilées)
      # ================================================
      cols = st.columns(2)
      with cols[0]:
          
        st.subheader("Répartition des vols par jour de la semaine")
        if not weekday_df.empty:
            mapping_jours = {1: "Dimanche", 2: "Lundi", 3: "Mardi", 4: "Mercredi", 5: "Jeudi", 6: "Vendredi", 7: "Samedi"}
            weekday_df["jour_nom"] = weekday_df["jour"].map(mapping_jours)
            df_long = pd.melt(weekday_df, id_vars=["jour_nom"], value_vars=["vols_a_temps", "vols_retard"],
                              var_name="Statut", value_name="Nombre de vols")
            df_long["Statut"] = df_long["Statut"].replace({"vols_a_temps": "À temps", "vols_retard": "En retard"})
            fig_weekday = px.bar(
                df_long,
                x="jour_nom",
                y="Nombre de vols",
                color="Statut",
                title="Répartition des vols par jour",
                labels={"jour_nom": "Jour"}
            )
            st.plotly_chart(fig_weekday, use_container_width=True)
        else:
            st.warning("Aucune donnée par jour de la semaine disponible.")

      # ================================================
      # 7. Distribution horaire des retards
      # ================================================
      with cols[1]:
        st.subheader("Distribution des retards par heure")
        if not hourly_df.empty:
            fig_hourly = px.line(
                hourly_df,
                x="hour",
                y=["retard_moyen_dep", "retard_moyen_arr"],
                labels={"hour": "Heure", "value": "Retard moyen (min)"},
                title="Distribution horaire des retards"
            )
            st.plotly_chart(fig_hourly, use_container_width=True)
        else:
            st.warning("Aucune donnée horaire disponible.")

      # ================================================
      # 8. Analyse des causes de retard (donut à double couche)
      # ================================================
      st.subheader("Répartition hiérarchique des causes de retard")
      cols = st.columns(2)
      with cols[0]:
          if not cause_dist_df.empty and not kpi_df.empty:
              total_flights = int(kpi_df["total_flights"].iloc[0])
              avg_compagnie = cause_dist_df["total_delay_compagnie"].fillna(0).iloc[0] / total_flights if total_flights > 0 else 0
              avg_meteo = cause_dist_df["total_delay_meteo"].fillna(0).iloc[0] / total_flights if total_flights > 0 else 0
              avg_NAS = cause_dist_df["total_delay_NAS"].fillna(0).iloc[0] / total_flights if total_flights > 0 else 0
              avg_securite = cause_dist_df["total_delay_securite"].fillna(0).iloc[0] / total_flights if total_flights > 0 else 0
              avg_avion = cause_dist_df["total_delay_avion_tardif"].fillna(0).iloc[0] / total_flights if total_flights > 0 else 0
              causes = ["Compagnie", "Météo", "NAS", "Sécurité", "Avion tardif"]
              valeurs = [avg_compagnie, avg_meteo, avg_NAS, avg_securite, avg_avion]
              couleurs = {"Compagnie": "#636EFA", "Météo": "#EF553B", "NAS": "#00CC96", "Sécurité": "#AB63FA", "Avion tardif": "#FFA15A"}
              fig_cause = go.Figure(data=[go.Pie(labels=causes, values=valeurs, hole=0.3,
                                                marker=dict(colors=[couleurs[c] for c in causes]))])
              fig_cause.update_layout(title="Répartition moyenne des causes (min/vol)")
              st.plotly_chart(fig_cause, use_container_width=True)
          else:
              st.warning("Aucune donnée sur les causes de retard.")

      # ================================================
      # 9. Évolution des causes de retard dans le temps
      # ================================================
      st.subheader("Tendance des causes de retard")
      if not cause_evol_df.empty:
          cause_evol_df["date"] = pd.to_datetime(cause_evol_df["date"])
          cause_evol_df = cause_evol_df.sort_values("date")
          cause_evol_df["avg_compagnie"] = cause_evol_df["total_delay_compagnie"].fillna(0) / cause_evol_df["vols"]
          cause_evol_df["avg_meteo"] = cause_evol_df["total_delay_meteo"].fillna(0) / cause_evol_df["vols"]
          cause_evol_df["avg_NAS"] = cause_evol_df["total_delay_NAS"].fillna(0) / cause_evol_df["vols"]
          cause_evol_df["avg_securite"] = cause_evol_df["total_delay_securite"].fillna(0) / cause_evol_df["vols"]
          cause_evol_df["avg_avion"] = cause_evol_df["total_delay_avion_tardif"].fillna(0) / cause_evol_df["vols"]
          cause_evol_df = cause_evol_df.rename(columns={
              "avg_compagnie": "Compagnie",
              "avg_meteo": "Météo",
              "avg_NAS": "NAS",
              "avg_securite": "Sécurité",
              "avg_avion": "Avion tardif"
          })
          color_map = {"Compagnie": "#636EFA", "Météo": "#EF553B", "NAS": "#00CC96", "Sécurité": "#AB63FA", "Avion tardif": "#FFA15A"}
          fig_cause_evol = px.line(
              cause_evol_df,
              x="date",
              y=["Compagnie", "Météo", "NAS", "Sécurité", "Avion tardif"],
              labels={"value": "Retard moyen (min/vol)", "date": "Date"},
              title="Tendance des causes de retard"
          )
          fig_cause_evol.update_layout(colorway=[color_map[c] for c in ["Compagnie", "Météo", "NAS", "Sécurité", "Avion tardif"]])
          st.plotly_chart(fig_cause_evol, use_container_width=True)
      else:
          st.warning("Aucune donnée d'évolution des causes de retard disponible.")

      loading_message.empty()

  else:
      st.info("Veuillez sélectionner vos filtres et cliquer sur 'Appliquer les filtres' pour afficher le dashboard.")
