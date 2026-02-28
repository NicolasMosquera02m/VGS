#!/usr/bin/env python3
"""
main.py - Script principal del ETL de Videojuegos
Orquesta el proceso completo de Extracción, Transformación y Carga
"""

import sys
import os
import logging
from datetime import datetime

# Agregar paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), 'extract'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'tranform'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'load'))

from extract import DataExtractor
from transform import DataTransformer
from load import DataLoader

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_execution.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def print_banner():
    """Imprime banner inicial del ETL"""
    banner = """
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║           ETL DE ANÁLISIS DE VIDEOJUEGOS                 ║
    ║           Backloggd Games Dataset                         ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_section(title):
    """Imprime encabezado de sección"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60 + "\n")


def main():
    """Función principal que ejecuta el pipeline ETL"""
    start_time = datetime.now()
    
    print_banner()
    logger.info("Iniciando proceso ETL...")
    
    try:
        # ==================== EXTRACT ====================
        print_section("FASE 1: EXTRACCIÓN DE DATOS")
        
        csv_path = 'backloggd_games.csv'
        extractor = DataExtractor(csv_path)
        df = extractor.extract_data()
        
        data_info = extractor.get_data_info(df)
        print(f"✓ Datos extraídos: {data_info['total_records']:,} registros")
        print(f"✓ Columnas: {len(data_info['columns'])}")
        
        # ==================== TRANSFORM ====================
        print_section("FASE 2: TRANSFORMACIÓN Y ANÁLISIS")
        
        transformer = DataTransformer(df)
        
        # Análisis 1: Juego más jugado
        print("→ Analizando juego más jugado...")
        most_played = transformer.get_most_played_game()
        print(f"  ✓ Juego más jugado: {most_played['title']}")
        print(f"    - Jugadas: {most_played['plays']}")
        print(f"    - Rating: {most_played['rating']:.2f}/5.0")
        
        # Análisis 2: Top 20 géneros más jugados
        print("\n→ Calculando top 20 géneros más jugados...")
        genres_df = transformer.get_top_genres(top_n=20)
        print(f"  ✓ Top 3 géneros:")
        for idx, row in genres_df.head(3).iterrows():
            print(f"    {idx+1}. {row['Genre']}: {row['Total_Plays']:,} jugadas")
        
        # Análisis 3: Ratings por género
        print("\n→ Analizando ratings por género...")
        ratings_summary = transformer.get_genre_rating_summary(genres_df)
        print(f"  ✓ Top 3 géneros mejor valorados:")
        for idx, row in ratings_summary.head(3).iterrows():
            print(f"    {idx+1}. {row['Genre']}: {row['Average_Rating']:.2f}/5.0")
        
        # Estadísticas generales
        print("\n→ Calculando estadísticas generales...")
        stats = transformer.get_summary_statistics()
        print(f"  ✓ Total de juegos: {stats['total_games']:,}")
        print(f"  ✓ Total de jugadas: {stats['total_plays']:,}")
        print(f"  ✓ Rating promedio global: {stats['average_rating']:.2f}/5.0")
        print(f"  ✓ Géneros únicos: {stats['unique_genres']}")
        
        # ==================== LOAD ====================
        print_section("FASE 3: GENERACIÓN DE VISUALIZACIONES")
        
        loader = DataLoader(output_dir='output')
        
        print("→ Generando reporte textual...")
        outputs = loader.generate_all_visualizations(
            most_played=most_played,
            stats=stats,
            genres_df=genres_df,
            ratings_summary=ratings_summary
        )
        
        print("\n✓ Archivos generados:")
        for name, path in outputs.items():
            print(f"  • {name}: {path}")
        
        # ==================== FINALIZACIÓN ====================
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print_section("ETL COMPLETADO EXITOSAMENTE")
        print(f"✓ Tiempo de ejecución: {duration:.2f} segundos")
        print(f"✓ Fecha: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nRevisa la carpeta 'output/' para ver todos los resultados.\n")
        
        logger.info(f"ETL completado exitosamente en {duration:.2f} segundos")
        return 0
        
    except Exception as e:
        logger.error(f"Error durante la ejecución del ETL: {str(e)}", exc_info=True)
        print(f"\n✗ ERROR: {str(e)}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
