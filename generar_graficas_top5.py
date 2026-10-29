#!/usr/bin/env python3
"""
generar_graficas_top5.py - Script para generar gráficas de top 5 juegos por categoría
Genera visualizaciones de los 5 juegos más jugados de las 6 categorías principales
"""

import logging
from analyze import VideoGameAnalyzer

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('graficas_top5.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Función principal que genera las gráficas de top 5 juegos"""
    
    print("\n" + "="*70)
    print("  GENERACIÓN DE GRÁFICAS: TOP 5 JUEGOS POR CATEGORÍA")
    print("="*70 + "\n")
    
    try:
        # Crear analizador
        logger.info("Inicializando analizador...")
        analyzer = VideoGameAnalyzer(csv_path='backloggd_games.csv', output_dir='output')
        
        # Fase 1: Extracción de datos
        print("📊 Extrayendo datos...")
        analyzer.extract_data()
        print(f"   ✓ Datos extraídos: {analyzer.results['extraction']['total_records']:,} registros\n")
        
        # Fase 2: Transformación inicial (necesaria para obtener los top géneros)
        print("🔄 Analizando datos...")
        analyzer.transform_and_analyze()
        
        # Mostrar las 6 categorías principales
        genres_df = analyzer.results['analysis']['genres_df']
        top_6_genres = genres_df.head(6)
        
        print("   ✓ Top 6 Categorías principales:")
        for idx, row in top_6_genres.iterrows():
            print(f"      {idx+1}. {row['Genre']}: {row['Total_Plays']:,} jugadas")
        print()
        
        # Fase 3: Análisis específico de top 5 juegos por categoría
        print("📈 Generando gráficas de top 5 juegos por categoría...")
        results = analyzer.analyze_top_games_by_categories(top_n_genres=6, top_n_games=5)
        
        # Mostrar resumen de juegos encontrados
        print("\n   ✓ Top 5 juegos por categoría:\n")
        for genre, games_df in results['top_games_dict'].items():
            print(f"   📂 {genre}:")
            for idx, (_, row) in enumerate(games_df.iterrows(), 1):
                print(f"      {idx}. {row['Title']}: {row['Plays']} jugadas (★{row['Rating']:.1f})")
            print()
        
        # Mostrar archivos generados
        print("   ✓ Gráficas generadas:")
        print(f"      • Gráfico combinado: {results['combined_chart']}")
        for chart in results['individual_charts']:
            print(f"      • {chart}")
        
        print("\n" + "="*70)
        print("  ✅ PROCESO COMPLETADO EXITOSAMENTE")
        print("="*70)
        print(f"\n📁 Todos los archivos guardados en: ./output/\n")
        
    except Exception as e:
        logger.error(f"Error durante la ejecución: {str(e)}", exc_info=True)
        print(f"\n❌ Error: {str(e)}\n")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
