import ast
import pickle
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass
class RentabilityModelArtifact:
    genre_stats_df: pd.DataFrame
    players_range: tuple[float, float]
    rating_range: tuple[float, float]
    global_median_budget: float = 1_500_000.0


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


def robust_normalize(value: float, low: float, high: float) -> float:
    if np.isclose(high, low) or np.isnan(low) or np.isnan(high):
        return 0.5
    return float(np.clip((value - low) / (high - low), 0.0, 1.0))


def load_dataset(path: str) -> pd.DataFrame:
    source_path = Path(path)
    if not source_path.exists():
        raise FileNotFoundError(f"No se encontro el dataset: {source_path}")

    if source_path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(source_path)
    else:
        df = pd.read_csv(source_path, index_col=0)

    df["Players_numeric"] = df["Plays"].apply(convert_plays_to_numeric)
    df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
    df["Genres_list"] = df["Genres"].apply(parse_genres)
    return df


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


def train_artifact_from_file(dataset_path: str) -> RentabilityModelArtifact:
    df = load_dataset(dataset_path)
    genre_stats_df = build_genre_stats(df)
    if genre_stats_df.empty:
        raise ValueError("No se pudieron calcular estadisticas por genero para entrenar el PKL.")

    players_q = genre_stats_df["median_players"].quantile([0.10, 0.90]).tolist()
    rating_q = genre_stats_df["avg_rating"].quantile([0.10, 0.90]).tolist()

    players_range = (float(players_q[0]), float(players_q[1]))
    rating_range = (float(rating_q[0]), float(rating_q[1]))

    return RentabilityModelArtifact(
        genre_stats_df=genre_stats_df,
        players_range=players_range,
        rating_range=rating_range,
    )


def save_artifact(artifact: RentabilityModelArtifact, output_path: str) -> None:
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as f:
        pickle.dump(artifact, f)


def load_artifact(path: str) -> RentabilityModelArtifact:
    with Path(path).open("rb") as f:
        artifact = pickle.load(f)

    if not isinstance(artifact, RentabilityModelArtifact):
        raise ValueError("El archivo PKL no contiene un artefacto valido de rentabilidad.")
    return artifact


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

    budget_ratio = max(budget_usd, 1.0) / max(global_median_budget, 1.0)
    budget_reach_factor = np.clip(np.log1p(budget_ratio) / np.log1p(6.0), 0.35, 1.35)
    rating_demand_factor = np.clip(0.75 + (avg_rating - 3.0) * 0.18, 0.50, 1.20)

    expected_players = base_avg_players * budget_reach_factor * marketing_multiplier * rating_demand_factor
    expected_revenue = expected_players * revenue_per_player
    estimated_profit_signed = expected_revenue - budget_usd
    roi_signed = estimated_profit_signed / max(budget_usd, 1.0)
    break_even_players = budget_usd / max(revenue_per_player, 1e-6)

    annual_growth = annual_growth_pct / 100.0
    annual_decay = annual_decay_pct / 100.0
    discount_rate = discount_rate_pct / 100.0
    net_change = annual_growth - annual_decay

    growth_year_index = np.arange(1, growth_years + 1)
    forecast_players = expected_players * np.power(1.0 + net_change, growth_year_index - 1)
    forecast_players = np.clip(forecast_players, a_min=0.0, a_max=None)
    forecast_revenue = forecast_players * revenue_per_player

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
