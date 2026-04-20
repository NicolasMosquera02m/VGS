import ast

import numpy as np
import pandas as pd
import streamlit as st

DATASET_PATH = "backloggd_games.csv"


def convert_plays_to_numeric(value) -> float:
    if pd.isna(value):
        return 0.0

    text = str(value).strip().upper().replace(",", "")
    if not text:
        return 0.0

    try:
        if text.endswith("K"):
            return float(text[:-1]) * 1_000
        if text.endswith("M"):
            return float(text[:-1]) * 1_000_000
        return float(text)
    except ValueError:
        return 0.0


def parse_genres(genres_str) -> list[str]:
    if pd.isna(genres_str):
        return []

    raw = str(genres_str).strip()
    if not raw:
        return []

    try:
        parsed = ast.literal_eval(raw)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except (ValueError, SyntaxError):
        pass

    return []


@st.cache_data
def load_dataset(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, index_col=0)
    df["Players_numeric"] = df["Plays"].apply(convert_plays_to_numeric)
    df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
    df["Genres_list"] = df["Genres"].apply(parse_genres)
    return df


@st.cache_data
def build_genre_stats(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for genre in sorted({g for genres in df["Genres_list"] for g in genres}):
        mask = df["Genres_list"].apply(lambda gs: genre in gs)
        subset = df.loc[mask]
        if subset.empty:
            continue

        rows.append(
            {
                "genre": genre,
                "games_count": int(len(subset)),
                "avg_players": float(subset["Players_numeric"].mean()),
                "median_players": float(subset["Players_numeric"].median()),
                "avg_rating": float(subset["Rating"].mean(skipna=True)),
            }
        )

    return pd.DataFrame(rows).sort_values("avg_players", ascending=False).reset_index(drop=True)


def robust_normalize(value: float, low: float, high: float) -> float:
    # Usa limites robustos para evitar que outliers dominen la escala.
    if np.isclose(high, low) or np.isnan(low) or np.isnan(high):
        return 0.5
    return float(np.clip((value - low) / (high - low), 0.0, 1.0))


def estimate_business_metrics(
    stats_row: pd.Series,
    budget_usd: float,
    global_median_budget: float,
    revenue_per_player: float,
    marketing_multiplier: float,
    development_years: int,
    growth_years: int,
    annual_growth_pct: float,
    annual_decay_pct: float,
    discount_rate_pct: float,
    players_range: tuple[float, float],
    rating_range: tuple[float, float],
) -> dict:
    base_avg_players = stats_row["avg_players"]
    base_median_players = stats_row["median_players"]
    avg_rating = stats_row["avg_rating"]

    # Presupuesto y rating se modelan por separado para evitar dependencia directa.
    budget_ratio = max(budget_usd, 1.0) / max(global_median_budget, 1.0)
    budget_reach_factor = np.clip(np.log1p(budget_ratio) / np.log1p(6.0), 0.35, 1.35)
    rating_demand_factor = np.clip(0.75 + (avg_rating - 3.0) * 0.18, 0.50, 1.20)

    expected_players = base_avg_players * budget_reach_factor * marketing_multiplier * rating_demand_factor
    expected_revenue = expected_players * revenue_per_player
    estimated_profit_signed = expected_revenue - budget_usd
    roi_signed = estimated_profit_signed / max(budget_usd, 1.0)
    break_even_players = budget_usd / max(revenue_per_player, 1e-6)

    # Proyeccion a futuro: crecimiento anual y decaimiento de interes.
    annual_growth = annual_growth_pct / 100.0
    annual_decay = annual_decay_pct / 100.0
    discount_rate = discount_rate_pct / 100.0
    net_change = annual_growth - annual_decay

    growth_year_index = np.arange(1, growth_years + 1)
    forecast_players = expected_players * np.power(1.0 + net_change, growth_year_index - 1)
    forecast_players = np.clip(forecast_players, a_min=0.0, a_max=None)
    forecast_revenue = forecast_players * revenue_per_player
    # Los ingresos arrancan despues de los anios de desarrollo.
    discount_periods = development_years + growth_year_index - 1
    discount_factors = np.power(1.0 + discount_rate, discount_periods)
    discounted_revenue = forecast_revenue / np.clip(discount_factors, 1e-6, None)

    lifetime_revenue = float(np.sum(forecast_revenue))
    lifetime_revenue_npv = float(np.sum(discounted_revenue))
    lifetime_profit_signed = lifetime_revenue_npv - budget_usd
    lifetime_roi_signed = lifetime_profit_signed / max(budget_usd, 1.0)

    popularity_norm = robust_normalize(base_median_players, players_range[0], players_range[1])
    rating_norm = robust_normalize(avg_rating, rating_range[0], rating_range[1])
    roi_norm = robust_normalize(lifetime_roi_signed, -1.0, 1.5)

    profitability_score = 0.60 * roi_norm + 0.25 * popularity_norm + 0.15 * rating_norm
    profitability_score = float(np.clip(np.round(profitability_score, 3), 0.0, 1.0))

    return {
        "expected_players": expected_players,
        "expected_revenue": expected_revenue,
        "estimated_profit": estimated_profit_signed,
        "roi": roi_signed,
        "estimated_profit_signed": estimated_profit_signed,
        "roi_signed": roi_signed,
        "lifetime_revenue": lifetime_revenue,
        "lifetime_revenue_npv": lifetime_revenue_npv,
        "lifetime_profit": lifetime_profit_signed,
        "lifetime_roi": lifetime_roi_signed,
        "lifetime_profit_signed": lifetime_profit_signed,
        "lifetime_roi_signed": lifetime_roi_signed,
        "break_even_players": break_even_players,
        "development_years": development_years,
        "growth_years": growth_years,
        "forecast_years": development_years + growth_year_index,
        "forecast_players": forecast_players,
        "forecast_revenue": forecast_revenue,
        "profitability_score": profitability_score,
    }


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


def build_suggestions(
    selected_genre: str,
    selected_row: pd.Series,
    estimation: dict,
    genre_stats_df: pd.DataFrame,
    budget_usd: float,
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

    st.title("Estimador de Presupuesto y Rentabilidad por Genero")
    st.write(
        "Selecciona un genero del dataset y ajusta el presupuesto para estimar ingresos, "
        "beneficio y una calificacion de rentabilidad de 0 a 1."
    )

    try:
        df = load_dataset(DATASET_PATH)
    except FileNotFoundError:
        st.error(f"No se encontro el archivo {DATASET_PATH} en la raiz del proyecto.")
        st.stop()

    genre_stats_df = build_genre_stats(df)
    if genre_stats_df.empty:
        st.error("No se pudieron calcular estadisticas por genero.")
        st.stop()

    # Rangos robustos para reducir sensibilidad a outliers.
    players_q = genre_stats_df["median_players"].quantile([0.10, 0.90]).tolist()
    rating_q = genre_stats_df["avg_rating"].quantile([0.10, 0.90]).tolist()
    players_range = (float(players_q[0]), float(players_q[1]))
    rating_range = (float(rating_q[0]), float(rating_q[1]))

    global_median_budget = 1_500_000.0

    with st.sidebar:
        st.header("Parametros")
        selected_genre = st.selectbox("Genero", options=genre_stats_df["genre"].tolist())
        budget_usd = st.number_input(
            "Presupuesto estimado (USD)",
            min_value=1.01,
            max_value=100_000_000.0,
            value=2_000_000.0,
            step=1.0,
            format="%.2f",
        )
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

    selected_row = genre_stats_df.loc[genre_stats_df["genre"] == selected_genre].iloc[0]

    estimation = estimate_business_metrics(
        stats_row=selected_row,
        budget_usd=budget_usd,
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
        f"Punto de equilibrio estimado: {estimation['break_even_players']:,.0f} jugadores para cubrir el presupuesto. "
        f"(con {development_years} anios de desarrollo y {growth_years} años de crecimiento)"
    )

    st.markdown("### Proyeccion anual")
    forecast_df = pd.DataFrame(
        {
            "Anio": estimation["forecast_years"],
            "Jugadores estimados": np.round(estimation["forecast_players"]).astype(int),
            "Ingreso estimado (USD)": np.round(estimation["forecast_revenue"], 0),
        }
    )
    st.dataframe(forecast_df, use_container_width=True)

    st.markdown("### Sugerencias para mejorar rentabilidad")
    suggestions = build_suggestions(
        selected_genre=selected_genre,
        selected_row=selected_row,
        estimation=estimation,
        genre_stats_df=genre_stats_df,
        budget_usd=budget_usd,
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
