#!/usr/bin/env python3
"""
main.py - Script principal del ETL de Videojuegos
Punto de entrada para el proceso completo de Extracción, Transformación y Carga
"""

import sys
import logging

from analyze import VideoGameAnalyzer

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
    print_banner()
    logger.info("Iniciando proceso ETL...")
    
    try:
        # Crear instancia del analizador
        analyzer = VideoGameAnalyzer(
            csv_path='backloggd_games.csv',
            output_dir='output'
        )
        
        # FASE 1: EXTRACCIÓN
        print_section("FASE 1: EXTRACCIÓN DE DATOS")
        analyzer.extract_data()
        extraction = analyzer.results['extraction']
        print(f"✓ Datos extraídos: {extraction['total_records']:,} registros")
        print(f"✓ Columnas: {extraction['columns_count']}")
        
        # FASE 2: TRANSFORMACIÓN
        print_section("FASE 2: TRANSFORMACIÓN Y ANÁLISIS")
        analyzer.transform_and_analyze()
        analysis = analyzer.results['analysis']
        
        # Mostrar resultados del análisis
        print("→ Analizando juego más jugado...")
        most_played = analysis['most_played']
        print(f"  ✓ Juego más jugado: {most_played['title']}")
        print(f"    - Jugadas: {most_played['plays']}")
        print(f"    - Rating: {most_played['rating']:.2f}/5.0")
        
        print("\n→ Calculando top 20 géneros más jugados...")
        genres_df = analysis['genres_df']
        print(f"  ✓ Top 3 géneros:")
        for idx, row in genres_df.head(3).iterrows():
            print(f"    {idx+1}. {row['Genre']}: {row['Total_Plays']:,} jugadas")
        
        print("\n→ Analizando ratings por género...")
        ratings_summary = analysis['ratings_summary']
        print(f"  ✓ Top 3 géneros mejor valorados:")
        for idx, row in ratings_summary.head(3).iterrows():
            print(f"    {idx+1}. {row['Genre']}: {row['Average_Rating']:.2f}/5.0")
        
        print("\n→ Calculando estadísticas generales...")
        stats = analysis['stats']
        print(f"  ✓ Total de juegos: {stats['total_games']:,}")
        print(f"  ✓ Total de jugadas: {stats['total_plays']:,}")
        print(f"  ✓ Rating promedio global: {stats['average_rating']:.2f}/5.0")
        print(f"  ✓ Géneros únicos: {stats['unique_genres']}")
        
        # FASE 3: CARGA
        print_section("FASE 3: GENERACIÓN DE VISUALIZACIONES")
        print("→ Generando reporte textual...")
        outputs = analyzer.load_results()
        
        print("\n✓ Archivos generados:")
        for name, path in outputs.items():
            print(f"  • {name}: {path}")
        
        # FINALIZACIÓN
        execution = analyzer.results['execution']
        print_section("ETL COMPLETADO EXITOSAMENTE")
        print(f"✓ Tiempo de ejecución: {execution['duration']:.2f} segundos")
        print(f"✓ Fecha: {execution['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nRevisa la carpeta 'output/' para ver todos los resultados.\n")
        
        logger.info(f"ETL completado exitosamente en {execution['duration']:.2f} segundos")
        return 0
        
    except Exception as e:
        logger.error(f"Error durante la ejecución del ETL: {str(e)}", exc_info=True)
        print(f"\n✗ ERROR: {str(e)}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
