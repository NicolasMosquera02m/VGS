#!/usr/bin/env python3
"""
analyze.py - Módulo de análisis del ETL de Videojuegos
Contiene la lógica principal del pipeline de análisis
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

logger = logging.getLogger(__name__)


class VideoGameAnalyzer:
    """Clase que encapsula todo el proceso de análisis de videojuegos"""
    
    def __init__(self, csv_path='backloggd_games.csv', output_dir='output'):
        """
        Inicializa el analizador de videojuegos
        
        Args:
            csv_path (str): Ruta al archivo CSV con los datos
            output_dir (str): Directorio donde se guardarán los resultados
        """
        self.csv_path = csv_path
        self.output_dir = output_dir
        self.results = {}
    
    def extract_data(self):
        """Fase 1: Extracción de datos"""
        logger.info("Iniciando fase de extracción...")
        
        self.extractor = DataExtractor(self.csv_path)
        self.df = self.extractor.extract_data()
        
        data_info = self.extractor.get_data_info(self.df)
        
        self.results['extraction'] = {
            'total_records': data_info['total_records'],
            'columns_count': len(data_info['columns']),
            'columns': data_info['columns']
        }
        
        logger.info(f"Extracción completada: {data_info['total_records']:,} registros")
        return self.df
    
    def transform_and_analyze(self):
        """Fase 2: Transformación y análisis de datos"""
        logger.info("Iniciando fase de transformación y análisis...")
        
        self.transformer = DataTransformer(self.df)
        
        # Análisis 1: Juego más jugado
        logger.info("Analizando juego más jugado...")
        most_played = self.transformer.get_most_played_game()
        
        # Análisis 2: Top 20 géneros más jugados
        logger.info("Calculando top 20 géneros más jugados...")
        genres_df = self.transformer.get_top_genres(top_n=20)
        
        # Análisis 3: Ratings por género
        logger.info("Analizando ratings por género...")
        ratings_summary = self.transformer.get_genre_rating_summary(genres_df)
        
        # Estadísticas generales
        logger.info("Calculando estadísticas generales...")
        stats = self.transformer.get_summary_statistics()
        
        self.results['analysis'] = {
            'most_played': most_played,
            'genres_df': genres_df,
            'ratings_summary': ratings_summary,
            'stats': stats
        }
        
        logger.info("Análisis completado exitosamente")
        return self.results['analysis']
    
    def load_results(self):
        """Fase 3: Generación de visualizaciones y reportes"""
        logger.info("Iniciando fase de carga y generación de reportes...")
        
        self.loader = DataLoader(output_dir=self.output_dir)
        
        analysis = self.results['analysis']
        outputs = self.loader.generate_all_visualizations(
            most_played=analysis['most_played'],
            stats=analysis['stats'],
            genres_df=analysis['genres_df'],
            ratings_summary=analysis['ratings_summary']
        )
        
        self.results['outputs'] = outputs
        
        logger.info(f"Generados {len(outputs)} archivos de salida")
        return outputs
    
    def analyze_top_games_by_categories(self, top_n_genres=6, top_n_games=5):
        """
        Analiza los top N juegos para las primeras N categorías
        
        Args:
            top_n_genres (int): Número de géneros principales a analizar
            top_n_games (int): Número de juegos por género
        
        Returns:
            dict: Diccionario con los resultados del análisis
        """
        logger.info(f"Analizando top {top_n_games} juegos para las {top_n_genres} categorías principales...")
        
        # Obtener las primeras 6 categorías del análisis existente
        genres_df = self.results['analysis']['genres_df']
        top_genres = genres_df.head(top_n_genres)['Genre'].tolist()
        
        logger.info(f"Categorías seleccionadas: {', '.join(top_genres)}")
        
        # Obtener top juegos para cada género
        top_games_dict = self.transformer.get_top_games_multiple_genres(top_genres, top_n_games)
        
        # Generar visualizaciones
        logger.info("Generando visualizaciones de top juegos...")
        
        # Crear loader si no existe
        if not hasattr(self, 'loader'):
            self.loader = DataLoader(output_dir=self.output_dir)
        
        # Gráficos individuales para cada género
        individual_charts = self.loader.plot_top_games_multiple_genres(top_games_dict)
        
        # Gráfico combinado
        combined_chart = self.loader.plot_combined_top_games(top_games_dict)
        
        self.results['top_games_analysis'] = {
            'top_genres': top_genres,
            'top_games_dict': top_games_dict,
            'individual_charts': individual_charts,
            'combined_chart': combined_chart
        }
        
        logger.info(f"Análisis de top juegos completado. Generados {len(individual_charts) + 1} gráficos")
        return self.results['top_games_analysis']
    
    def run_full_pipeline(self):
        """
        Ejecuta el pipeline completo de análisis
        
        Returns:
            dict: Diccionario con todos los resultados del análisis
        """
        start_time = datetime.now()
        logger.info("Iniciando pipeline completo de análisis...")
        
        try:
            # Fase 1: Extracción
            self.extract_data()
            
            # Fase 2: Transformación y análisis
            self.transform_and_analyze()
            
            # Fase 3: Carga y visualización
            self.load_results()
            
            # Tiempo de ejecución
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.results['execution'] = {
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration,
                'status': 'success'
            }
            
            logger.info(f"Pipeline completado exitosamente en {duration:.2f} segundos")
            return self.results
            
        except Exception as e:
            logger.error(f"Error durante la ejecución del pipeline: {str(e)}", exc_info=True)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.results['execution'] = {
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration,
                'status': 'error',
                'error': str(e)
            }
            
            raise
    
    def print_results_summary(self):
        """Imprime un resumen de los resultados del análisis"""
        if 'analysis' not in self.results:
            print("No hay resultados disponibles para mostrar.")
            return
        
        analysis = self.results['analysis']
        extraction = self.results['extraction']
        
        print("\n" + "="*60)
        print("  RESUMEN DE RESULTADOS")
        print("="*60 + "\n")
        
        # Datos extraídos
        print(f"✓ Datos extraídos: {extraction['total_records']:,} registros")
        print(f"✓ Columnas: {extraction['columns_count']}")
        
        # Juego más jugado
        most_played = analysis['most_played']
        print(f"\n✓ Juego más jugado: {most_played['title']}")
        print(f"  - Jugadas: {most_played['plays']}")
        print(f"  - Rating: {most_played['rating']:.2f}/5.0")
        
        # Top géneros
        genres_df = analysis['genres_df']
        print(f"\n✓ Top 3 géneros más jugados:")
        for idx, row in genres_df.head(3).iterrows():
            print(f"  {idx+1}. {row['Genre']}: {row['Total_Plays']:,} jugadas")
        
        # Ratings por género
        ratings_summary = analysis['ratings_summary']
        print(f"\n✓ Top 3 géneros mejor valorados:")
        for idx, row in ratings_summary.head(3).iterrows():
            print(f"  {idx+1}. {row['Genre']}: {row['Average_Rating']:.2f}/5.0")
        
        # Estadísticas generales
        stats = analysis['stats']
        print(f"\n✓ Estadísticas generales:")
        print(f"  - Total de juegos: {stats['total_games']:,}")
        print(f"  - Total de jugadas: {stats['total_plays']:,}")
        print(f"  - Rating promedio: {stats['average_rating']:.2f}/5.0")
        print(f"  - Géneros únicos: {stats['unique_genres']}")
        
        # Archivos generados
        if 'outputs' in self.results:
            print(f"\n✓ Archivos generados:")
            for name, path in self.results['outputs'].items():
                print(f"  • {name}: {path}")
        
        # Tiempo de ejecución
        if 'execution' in self.results:
            execution = self.results['execution']
            print(f"\n✓ Tiempo de ejecución: {execution['duration']:.2f} segundos")
            print(f"✓ Fecha: {execution['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")


def run_analysis(csv_path='backloggd_games.csv', output_dir='output', verbose=True):
    """
    Función de conveniencia para ejecutar el análisis completo
    
    Args:
        csv_path (str): Ruta al archivo CSV
        output_dir (str): Directorio de salida
        verbose (bool): Si se debe imprimir información detallada
    
    Returns:
        dict: Resultados del análisis
    """
    analyzer = VideoGameAnalyzer(csv_path, output_dir)
    results = analyzer.run_full_pipeline()
    
    if verbose:
        analyzer.print_results_summary()
    
    return results
