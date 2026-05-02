from pathlib import Path
import time

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from model.rentability_model import (
    estimate_business_metrics,
    load_artifact,
    save_artifact,
    train_artifact_from_file,
)

PKL_PATH = "model/rentability_model.pkl"
DATASET_CANDIDATES = ["backloggd_games.xlsx", "backloggd_games.xls", "backloggd_games.csv"]


def find_dataset_source() -> str | None:
    for candidate in DATASET_CANDIDATES:
        if Path(candidate).exists():
            return candidate
    return None


@st.cache_resource
def get_model_artifact(pkl_path: str):
    model_path = Path(pkl_path)
    if model_path.exists():
        return load_artifact(pkl_path)

    dataset_path = find_dataset_source()
    if not dataset_path:
        raise FileNotFoundError(
            "No existe PKL y no se encontro dataset fuente (.xlsx/.xls/.csv) para generarlo automaticamente."
        )

    artifact = train_artifact_from_file(dataset_path)
    save_artifact(artifact, pkl_path)
    return artifact


def score_label(score: float) -> str:
    if score < 0.4:
        return "No rentable"
    if score < 0.6:
        return "Riesgo alto"
    if score < 0.75:
        return "Rentabilidad moderada"
    if score < 0.9:
        return "Rentable"
    return "Muy rentable"


def score_color(score: float) -> str:
    if score < 0.6:
        return "#d62828"
    return "#2a9d8f"


def roi_color(roi: float) -> str:
    if roi < 0:
        return "#d62828"
    return "#2a9d8f"


def initialize_simulation_state() -> None:
    if "simulation_enabled" not in st.session_state:
        st.session_state.simulation_enabled = True
    if "simulation_step" not in st.session_state:
        st.session_state.simulation_step = 1
    if "simulation_speed_seconds" not in st.session_state:
        st.session_state.simulation_speed_seconds = 1.0
    if "simulation_last_tick" not in st.session_state:
        st.session_state.simulation_last_tick = time.time()
    if "scenario_signature" not in st.session_state:
        st.session_state.scenario_signature = None


@st.fragment(run_every=1)
def render_live_simulation(
    estimation: dict,
    total_budget_usd: float,
    selected_genre: str,
    development_years: int,
    growth_years: int,
) -> None:
    max_step = len(estimation["forecast_years"])
    current_step = int(np.clip(st.session_state.simulation_step, 1, max_step))

    if st.session_state.simulation_enabled and current_step < max_step:
        now = time.time()
        elapsed = now - float(st.session_state.simulation_last_tick)
        speed_seconds = max(float(st.session_state.simulation_speed_seconds), 0.5)
        if elapsed >= speed_seconds:
            steps_to_advance = max(1, int(elapsed // speed_seconds))
            current_step = min(max_step, current_step + steps_to_advance)
            st.session_state.simulation_step = current_step
            st.session_state.simulation_last_tick = now
    else:
        st.session_state.simulation_last_tick = time.time()

    visible_years = estimation["forecast_years"][:current_step]
    visible_revenue = estimation["forecast_revenue"][:current_step]
    visible_players = estimation["forecast_players"][:current_step]
    cumulative_revenue = np.cumsum(visible_revenue)
    cumulative_profit = cumulative_revenue - total_budget_usd
    current_revenue = float(visible_revenue[-1])
    current_players = float(visible_players[-1])
    current_cumulative_revenue = float(cumulative_revenue[-1])
    current_cumulative_profit = float(cumulative_profit[-1])
    current_roi = current_cumulative_profit / max(total_budget_usd, 1.0)
    current_calendar_year = pd.Timestamp.now().year + int(visible_years[-1]) - 1

    st.markdown("### Simulación en vivo")
    status_label = "En reproducción" if st.session_state.simulation_enabled else "Pausada"
    st.caption(
        f"Estado: {status_label} | Paso {current_step}/{max_step} | Género: {selected_genre}"
    )

    a, b, c, d = st.columns(4)
    a.metric("Año de simulación", str(current_calendar_year))
    b.metric("Jugadores actuales", f"{current_players:,.0f}")
    c.metric("Ingreso acumulado", f"USD {current_cumulative_revenue:,.0f}")
    d.metric("ROI acumulado", f"{current_roi * 100:.1f}%")

    sim_fig = go.Figure()
    sim_fig.add_trace(
        go.Scatter(
            x=estimation["forecast_years"],
            y=np.cumsum(estimation["forecast_revenue"]),
            mode="lines",
            name="Trayectoria completa",
            line=dict(color="#9aa0a6", dash="dot"),
            hovertemplate="Paso %{x}<br>Ingreso acumulado USD %{y:,.0f}<extra></extra>",
        )
    )
    sim_fig.add_trace(
        go.Scatter(
            x=visible_years,
            y=cumulative_revenue,
            mode="lines+markers",
            name="Simulación actual",
            line=dict(color="#2a9d8f", width=3),
            marker=dict(size=9, color="#2a9d8f"),
            hovertemplate="Paso %{x}<br>Ingreso acumulado USD %{y:,.0f}<extra></extra>",
        )
    )
    sim_fig.add_trace(
        go.Scatter(
            x=[visible_years[-1]],
            y=[current_cumulative_revenue],
            mode="markers+text",
            name="Punto actual",
            marker=dict(size=16, color="#d62828", symbol="circle-open"),
            text=[f"USD {current_cumulative_revenue:,.0f}"],
            textposition="top center",
            hovertemplate="Paso %{x}<br>Punto actual USD %{y:,.0f}<extra></extra>",
        )
    )
    sim_fig.add_hline(y=total_budget_usd, line_dash="dash", line_color="#d62828")
    sim_fig.update_layout(
        title="Simulación temporal de rentabilidad",
        xaxis_title="Paso del proyecto",
        yaxis_title="Ingreso acumulado (USD)",
        hovermode="x unified",
    )
    st.plotly_chart(sim_fig, use_container_width=True)

    sim_table = pd.DataFrame(
        {
            "Paso": visible_years,
            "Jugadores": np.round(visible_players).astype(int),
            "Ingreso anual (USD)": np.round(visible_revenue, 0),
            "Ingreso acumulado (USD)": np.round(cumulative_revenue, 0),
            "Beneficio acumulado (USD)": np.round(cumulative_profit, 0),
        }
    )
    st.dataframe(sim_table, use_container_width=True)

    st.caption(
        f"La simulación usa el mismo algoritmo del PKL y avanza cada {st.session_state.simulation_speed_seconds:.1f} segundos por paso."
    )


def build_suggestions(
    selected_genre: str,
    selected_row: pd.Series,
    estimation: dict,
    genre_stats_df: pd.DataFrame,
    budget_usd: float,
    contingency_pct: float,
    revenue_per_player: float,
) -> list[str]:
    suggestions = []

    if estimation["lifetime_roi_signed"] >= 0:
        suggestions.append(
            f"El genero {selected_genre} ya muestra rentabilidad positiva a futuro. Puedes escalar marketing de forma gradual para no subir el riesgo."
        )
        return suggestions

    max_budget_for_break_even = estimation["lifetime_revenue_npv"]
    required_revenue_per_player = budget_usd / max(np.sum(estimation["forecast_players"]), 1e-6)

    suggestions.append(
        f"Con la opcion {selected_genre}, el presupuesto actual es alto para el retorno esperado. Presupuesto sugerido para equilibrio: USD {max_budget_for_break_even:,.0f}."
    )
    suggestions.append(
        f"Para sostener este presupuesto en {selected_genre}, el ingreso por jugador deberia subir a ~USD {required_revenue_per_player:,.2f} (actual: USD {revenue_per_player:,.2f})."
    )

    if contingency_pct > 0:
        suggestions.append(
            f"Estas usando un colchon de imprevistos de {contingency_pct:.1f}%. Mantenlo protegido y libera ese monto por hitos segun avance real del proyecto."
        )

    if selected_row["avg_rating"] < 3.8:
        suggestions.append(
            "El rating historico del genero es moderado. Prioriza calidad del juego (bugs, UX, retencion) antes de escalar adquisicion de usuarios."
        )

    top_alternatives = genre_stats_df.sort_values(["avg_players", "avg_rating"], ascending=False).head(3)["genre"].tolist()
    if selected_genre in top_alternatives:
        top_alternatives = genre_stats_df.sort_values(["avg_rating", "avg_players"], ascending=False).head(4)["genre"].tolist()

    alternatives_text = ", ".join([g for g in top_alternatives if g != selected_genre][:3])
    if alternatives_text:
        suggestions.append(
            f"Si mantienes la misma inversion, evalua pruebas de concepto en generos con mejor traccion historica: {alternatives_text}."
        )

    suggestions.append(
        "Divide el presupuesto en hitos (MVP, beta, lanzamiento). Si un hito no cumple KPIs de conversion/retencion, detiene gasto adicional."
    )
    return suggestions


def main():
    st.set_page_config(page_title="Estimador de Rentabilidad de Juegos", layout="wide")
    initialize_simulation_state()

    st.title("Plataforma de Rentabilidad con Modelo PKL")
    st.write(
        "La app usa un artefacto PKL entrenado con el algoritmo actual. "
        "Las graficas y metricas se actualizan en tiempo real al mover los controles."
    )

    try:
        artifact = get_model_artifact(PKL_PATH)
    except FileNotFoundError:
        st.error(
            "No se encontro PKL ni dataset fuente para autoentrenamiento. "
            "Agrega un archivo backloggd_games.xlsx/.xls/.csv en la raiz o ejecuta train_profitability_pkl.py"
        )
        st.stop()

    genre_stats_df = artifact.genre_stats_df
    if genre_stats_df.empty:
        st.error("No se pudieron calcular estadisticas por genero.")
        st.stop()

    players_range = artifact.players_range
    rating_range = artifact.rating_range
    global_median_budget = artifact.global_median_budget

    with st.sidebar:
        st.header("Parametros")
        st.caption(f"Fuente activa: {PKL_PATH}")
        if st.button("Reentrenar PKL automaticamente"):
            dataset_path = find_dataset_source()
            if not dataset_path:
                st.error("No hay dataset .xlsx/.xls/.csv disponible para reentrenar.")
            else:
                retrained = train_artifact_from_file(dataset_path)
                save_artifact(retrained, PKL_PATH)
                get_model_artifact.clear()
                st.success(f"PKL reentrenado desde {dataset_path}")
                st.rerun()

        selected_genre = st.selectbox("Genero", options=sorted(genre_stats_df["genre"].tolist()))
        budget_base_usd = st.number_input(
            "Presupuesto normal (USD)",
            min_value=1.0,
            value=2_000_000.0,
            step=1.0,
            format="%.2f",
        )
        contingency_pct = st.slider(
            "Colchon para imprevistos (%)",
            min_value=0.0,
            max_value=40.0,
            value=10.0,
            step=0.5,
        )
        contingency_amount_usd = budget_base_usd * contingency_pct / 100.0
        st.caption(f"Presupuesto normal: USD {budget_base_usd:,.2f}")
        st.caption(f"Colchon para riesgos: USD {contingency_amount_usd:,.2f}")
        revenue_per_player = st.slider(
            "Ingreso estimado por jugador (USD)",
            min_value=0.1,
            max_value=10.0,
            value=1.8,
            step=0.1,
        )
        marketing_multiplier = st.slider(
            "Multiplicador de marketing",
            min_value=0.6,
            max_value=1.8,
            value=1.0,
            step=0.05,
        )
        development_years = st.slider(
            "Años de desarrollo",
            min_value=1,
            max_value=8,
            value=2,
            step=1,
        )
        growth_years = st.slider(
            "Años de crecimiento comercial",
            min_value=1,
            max_value=12,
            value=5,
            step=1,
        )
        annual_growth_pct = st.slider(
            "Crecimiento anual esperado de jugadores (%)",
            min_value=-20.0,
            max_value=40.0,
            value=8.0,
            step=1.0,
        )
        annual_decay_pct = st.slider(
            "Decaimiento anual por desgaste (%)",
            min_value=0.0,
            max_value=30.0,
            value=6.0,
            step=1.0,
        )
        discount_rate_pct = st.slider(
            "Tasa de descuento anual (%)",
            min_value=0.0,
            max_value=30.0,
            value=10.0,
            step=1.0,
        )

        st.divider()
        st.subheader("Simulacion en vivo")
        st.session_state.simulation_enabled = st.toggle(
            "Reproducir simulación",
            value=st.session_state.simulation_enabled,
        )
        st.session_state.simulation_speed_seconds = st.slider(
            "Velocidad de avance (segundos por paso)",
            min_value=0.5,
            max_value=5.0,
            value=float(st.session_state.simulation_speed_seconds),
            step=0.5,
        )
        if st.button("Reiniciar simulación"):
            st.session_state.simulation_step = 1
            st.session_state.simulation_last_tick = time.time()

    scenario_signature = (
        selected_genre,
        budget_base_usd,
        contingency_pct,
        revenue_per_player,
        marketing_multiplier,
        development_years,
        growth_years,
        annual_growth_pct,
        annual_decay_pct,
        discount_rate_pct,
    )
    if st.session_state.scenario_signature != scenario_signature:
        st.session_state.scenario_signature = scenario_signature
        st.session_state.simulation_step = 1
        st.session_state.simulation_last_tick = time.time()

    selected_row = genre_stats_df.loc[genre_stats_df["genre"] == selected_genre].iloc[0]
    total_budget_usd = budget_base_usd + contingency_amount_usd

    estimation = estimate_business_metrics(
        stats_row=selected_row,
        budget_usd=total_budget_usd,
        global_median_budget=global_median_budget,
        revenue_per_player=revenue_per_player,
        marketing_multiplier=marketing_multiplier,
        development_years=development_years,
        growth_years=growth_years,
        annual_growth_pct=annual_growth_pct,
        annual_decay_pct=annual_decay_pct,
        discount_rate_pct=discount_rate_pct,
        players_range=players_range,
        rating_range=rating_range,
    )

    score = estimation["profitability_score"]
    color = score_color(score)
    roi = estimation["lifetime_roi"]
    roi_signed = estimation["lifetime_roi_signed"]
    roi_card_color = roi_color(roi_signed)
    total_horizon = development_years + growth_years

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Calificacion (0-1)", f"{score:.3f}")
    c2.metric(f"ROI a {total_horizon} años", f"{roi * 100:.1f}%")
    c3.metric("Ingreso futuro (NPV)", f"USD {estimation['lifetime_revenue_npv']:,.0f}")
    c4.metric("Beneficio futuro", f"USD {estimation['lifetime_profit']:,.0f}")

    c5, c6, c7 = st.columns(3)
    c5.metric("Presupuesto normal", f"USD {budget_base_usd:,.0f}")
    c6.metric("Colchon de riesgos", f"USD {contingency_amount_usd:,.0f}")
    c7.metric("Presupuesto total", f"USD {total_budget_usd:,.0f}")

    st.markdown(
        f"""
        <div style='padding:0.6rem 0.9rem;border-radius:0.5rem;background:{roi_card_color};color:white;font-weight:700;margin-bottom:0.7rem;'>
            ROI futuro: {roi * 100:.1f}% | {'No recupera inversion' if roi_signed < 0 else 'Recupera inversion'}
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div style='padding:0.8rem 1rem;border-radius:0.6rem;background:{color};color:white;font-weight:700;'>
            Diagnostico: {score_label(score)} | Score: {score:.3f} (0-1)
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.progress(min(score, 1.0))
    st.caption("Rojo: no rentable. Verde: rentable.")

    st.info(
        f"Punto de equilibrio estimado: {estimation['break_even_players']:,.0f} jugadores para cubrir el presupuesto total. "
        f"(con {development_years} años de desarrollo y {growth_years} años de crecimiento)"
    )

    st.markdown("### Proyección anual")
    current_year = pd.Timestamp.now().year
    forecast_df = pd.DataFrame(
        {
            "Año": estimation["forecast_years"],
            "Año calendario": current_year + estimation["forecast_years"] - 1,
            "Jugadores estimados": np.round(estimation["forecast_players"]).astype(int),
            "Ingreso estimado (USD)": np.round(estimation["forecast_revenue"], 0),
        }
    )

    cumulative_revenue = np.cumsum(estimation["forecast_revenue"])
    cumulative_profit = cumulative_revenue - total_budget_usd
    roi_by_year = cumulative_profit / max(total_budget_usd, 1.0)

    profitability_df = pd.DataFrame(
        {
            "Año": estimation["forecast_years"],
            "Año calendario": current_year + estimation["forecast_years"] - 1,
            "Ingreso anual (USD)": np.round(estimation["forecast_revenue"], 0),
            "Ingreso acumulado (USD)": np.round(cumulative_revenue, 0),
            "Beneficio acumulado (USD)": np.round(cumulative_profit, 0),
            "ROI acumulado (%)": np.round(roi_by_year * 100, 2),
        }
    )
    st.dataframe(forecast_df, use_container_width=True)

    st.markdown("### Gráficas de rentabilidad del proyecto")
    st.caption("Curvas interactivas con puntos por año y datos de hover en tiempo real.")

    x_axis = profitability_df["Año calendario"]

    income_fig = go.Figure()
    income_fig.add_trace(
        go.Scatter(
            x=x_axis,
            y=profitability_df["Ingreso anual (USD)"],
            mode="lines+markers",
            name="Ingreso anual",
            hovertemplate="Año %{x}<br>Ingreso anual USD %{y:,.0f}<extra></extra>",
        )
    )
    income_fig.add_trace(
        go.Scatter(
            x=x_axis,
            y=profitability_df["Ingreso acumulado (USD)"],
            mode="lines+markers",
            name="Ingreso acumulado",
            hovertemplate="Año %{x}<br>Ingreso acumulado USD %{y:,.0f}<extra></extra>",
        )
    )
    income_fig.update_layout(
        title="Ingresos del proyecto en el tiempo",
        xaxis_title="Año calendario",
        yaxis_title="USD",
        hovermode="x unified",
    )
    st.plotly_chart(income_fig, use_container_width=True)

    profit_fig = go.Figure()
    profit_fig.add_trace(
        go.Bar(
            x=x_axis,
            y=profitability_df["Beneficio acumulado (USD)"],
            name="Beneficio acumulado",
            hovertemplate="Año %{x}<br>Beneficio acumulado USD %{y:,.0f}<extra></extra>",
        )
    )
    profit_fig.update_layout(
        title="Impacto acumulado en la rentabilidad",
        xaxis_title="Año calendario",
        yaxis_title="USD",
    )
    st.plotly_chart(profit_fig, use_container_width=True)

    roi_fig = go.Figure()
    roi_fig.add_trace(
        go.Scatter(
            x=x_axis,
            y=profitability_df["ROI acumulado (%)"],
            mode="lines+markers",
            name="ROI acumulado",
            hovertemplate="Año %{x}<br>ROI %{y:.2f}%<extra></extra>",
        )
    )
    roi_fig.add_hline(y=0, line_dash="dash", line_color="#d62828")
    roi_fig.update_layout(
        title="ROI acumulado por año",
        xaxis_title="Año calendario",
        yaxis_title="ROI (%)",
    )
    st.plotly_chart(roi_fig, use_container_width=True)

    st.markdown("### Tabla consolidada de rentabilidad")
    st.dataframe(profitability_df, use_container_width=True)

    render_live_simulation(
        estimation=estimation,
        total_budget_usd=total_budget_usd,
        selected_genre=selected_genre,
        development_years=development_years,
        growth_years=growth_years,
    )

    st.markdown("### Sugerencias para mejorar rentabilidad")
    suggestions = build_suggestions(
        selected_genre=selected_genre,
        selected_row=selected_row,
        estimation=estimation,
        genre_stats_df=genre_stats_df,
        budget_usd=total_budget_usd,
        contingency_pct=contingency_pct,
        revenue_per_player=revenue_per_player,
    )
    for idx, suggestion in enumerate(suggestions, start=1):
        st.write(f"{idx}. {suggestion}")

    st.markdown("### Contexto del genero seleccionado")
    st.write(
        pd.DataFrame(
            {
                "Genero": [selected_row["genre"]],
                "Juegos en dataset": [int(selected_row["games_count"])],
                "Jugadores promedio": [f"{selected_row['avg_players']:,.0f}"],
                "mediana de Jugadores ": [f"{selected_row['median_players']:,.0f}"],
                "Rating promedio": [f"{selected_row['avg_rating']:.2f}"],
            }
        )
    )

    st.markdown("### Top 10 generos por jugadores promedio")
    st.dataframe(
        genre_stats_df.head(10).rename(
            columns={
                "genre": "Genero",
                "games_count": "Juegos",
                "avg_players": "Jugadores promedio",
                "median_players": "Jugadores mediana",
                "avg_rating": "Rating promedio",
            }
        ),
        use_container_width=True,
    )

    with st.expander("Como se calcula la calificacion"):
        st.write(
            "La calificacion final (0-1) combina ROI estimado, popularidad historica del genero "
            "y rating promedio del genero. Es una estimacion orientativa basada en el dataset."
        )
        st.write(
            "ROI estimado = (Ganancia futura descontada - Presupuesto) / Presupuesto. "
            "Puede ser negativo (perdida) o positivo (ganancia)."
        )


if __name__ == "__main__":
    main()
