#!/usr/bin/env python3
"""
Entrena y guarda el artefacto PKL de rentabilidad para la app Streamlit.
"""

import argparse
from pathlib import Path

from model.rentability_model import save_artifact, train_artifact_from_file


DEFAULT_SOURCES = [
    "backloggd_games.xlsx",
    "backloggd_games.xls",
    "backloggd_games.csv",
]


def resolve_dataset_path(user_path: str | None) -> str:
    if user_path:
        return user_path

    for candidate in DEFAULT_SOURCES:
        if Path(candidate).exists():
            return candidate

    raise FileNotFoundError(
        "No se encontro dataset fuente. Usa --dataset con un archivo .xlsx/.xls/.csv valido."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Entrenar PKL de rentabilidad")
    parser.add_argument("--dataset", type=str, default=None, help="Ruta al dataset .xlsx/.xls/.csv")
    parser.add_argument(
        "--output",
        type=str,
        default="model/rentability_model.pkl",
        help="Ruta de salida del artefacto PKL",
    )
    args = parser.parse_args()

    dataset_path = resolve_dataset_path(args.dataset)
    artifact = train_artifact_from_file(dataset_path)
    save_artifact(artifact, args.output)

    print(f"Dataset fuente: {dataset_path}")
    print(f"PKL generado: {args.output}")
    print(f"Generos entrenados: {len(artifact.genre_stats_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
