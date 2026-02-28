"""
transform.py - Módulo de Transformación del ETL
Procesa y transforma los datos extraídos para análisis
"""

import pandas as pd
import ast
import logging
from collections import Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataTransformer:
    """Clase para transformar y analizar datos de videojuegos"""
    
    def __init__(self, df):
        """
        Inicializa el transformador con un DataFrame
        
        Args:
            df (pd.DataFrame): DataFrame con los datos a transformar
        """
        self.df = df.copy()
        self._clean_data()
        
    def _clean_data(self):
        """Limpia y prepara los datos para análisis"""
        logger.info("Limpiando datos...")
        
        # Convertir Plays a valores numéricos (eliminar 'K' y convertir)
        self.df['Plays_numeric'] = self.df['Plays'].apply(self._convert_plays_to_numeric)
        
        # Convertir Rating a numérico
        self.df['Rating'] = pd.to_numeric(self.df['Rating'], errors='coerce')
        
        # Parsear géneros (están en formato de lista como string)
        self.df['Genres_list'] = self.df['Genres'].apply(self._parse_genres)
        
        logger.info("Limpieza de datos completada")
    
    def _convert_plays_to_numeric(self, plays_str):
        """
        Convierte el string de Plays a valor numérico
        
        Args:
            plays_str (str): String con el número de plays (ej: "21K")
            
        Returns:
            int: Valor numérico de plays
        """
        try:
            if pd.isna(plays_str):
                return 0
            plays_str = str(plays_str).strip()
            if 'K' in plays_str:
                return int(float(plays_str.replace('K', '')) * 1000)
            return int(plays_str)
        except:
            return 0
    
    def _parse_genres(self, genres_str):
        """
        Parsea el string de géneros a lista
        
        Args:
            genres_str (str): String con lista de géneros
            
        Returns:
            list: Lista de géneros
        """
        try:
            if pd.isna(genres_str):
                return []
            # Convertir string de lista a lista real
            genres = ast.literal_eval(genres_str)
            return genres if isinstance(genres, list) else []
        except:
            return []
    
    def get_most_played_game(self):
        """
        Encuentra el juego más jugado
        
        Returns:
            dict: Información del juego más jugado
        """
        logger.info("Buscando el juego más jugado...")
        most_played = self.df.loc[self.df['Plays_numeric'].idxmax()]
        
        result = {
            'title': most_played['Title'],
            'plays': most_played['Plays'],
            'plays_numeric': most_played['Plays_numeric'],
            'rating': most_played['Rating'],
            'genres': most_played['Genres'],
            'platforms': most_played['Platforms'],
            'release_date': most_played['Release_Date']
        }
        
        logger.info(f"Juego más jugado: {result['title']} con {result['plays']} jugadas")
        return result
    
    def get_top_genres(self, top_n=20):
        """
        Obtiene los géneros más jugados
        
        Args:
            top_n (int): Número de géneros a devolver
            
        Returns:
            pd.DataFrame: DataFrame con géneros y total de jugadas
        """
        logger.info(f"Calculando top {top_n} géneros más jugados...")
        
        # Crear lista de tuplas (género, plays)
        genre_plays = []
        for idx, row in self.df.iterrows():
            genres = row['Genres_list']
            plays = row['Plays_numeric']
            for genre in genres:
                genre_plays.append((genre, plays))
        
        # Agrupar por género y sumar plays
        genre_dict = {}
        for genre, plays in genre_plays:
            if genre not in genre_dict:
                genre_dict[genre] = 0
            genre_dict[genre] += plays
        
        # Convertir a DataFrame y ordenar
        genres_df = pd.DataFrame(list(genre_dict.items()), columns=['Genre', 'Total_Plays'])
        genres_df = genres_df.sort_values('Total_Plays', ascending=False).head(top_n)
        
        logger.info(f"Top {top_n} géneros calculados")
        return genres_df
    
    def get_top_rated_by_genre(self, genres_df, top_n=10):
        """
        Obtiene los juegos mejor valorados para los géneros principales
        
        Args:
            genres_df (pd.DataFrame): DataFrame con los géneros principales
            top_n (int): Número de juegos por género
            
        Returns:
            pd.DataFrame: DataFrame con juegos mejor valorados por género
        """
        logger.info("Calculando juegos mejor valorados por género...")
        
        top_genres_list = genres_df['Genre'].tolist()
        
        # Filtrar juegos que pertenecen a los géneros principales
        genre_ratings = []
        
        for genre in top_genres_list:
            # Filtrar juegos que contienen este género
            genre_games = self.df[self.df['Genres_list'].apply(lambda x: genre in x)]
            
            # Obtener top juegos por rating
            top_games = genre_games.nlargest(top_n, 'Rating')
            
            for idx, game in top_games.iterrows():
                genre_ratings.append({
                    'Genre': genre,
                    'Title': game['Title'],
                    'Rating': game['Rating'],
                    'Plays': game['Plays_numeric']
                })
        
        ratings_df = pd.DataFrame(genre_ratings)
        logger.info(f"Análisis de valoraciones por género completado")
        return ratings_df
    
    def get_genre_rating_summary(self, genres_df):
        """
        Obtiene un resumen de ratings promedio por género
        
        Args:
            genres_df (pd.DataFrame): DataFrame con los géneros principales
            
        Returns:
            pd.DataFrame: DataFrame con género y rating promedio
        """
        logger.info("Calculando ratings promedio por género...")
        
        top_genres_list = genres_df['Genre'].tolist()
        genre_avg_ratings = []
        
        for genre in top_genres_list:
            # Filtrar juegos que contienen este género
            genre_games = self.df[self.df['Genres_list'].apply(lambda x: genre in x)]
            
            # Calcular rating promedio (solo juegos con rating válido)
            valid_ratings = genre_games[genre_games['Rating'].notna()]
            
            if len(valid_ratings) > 0:
                avg_rating = valid_ratings['Rating'].mean()
                game_count = len(genre_games)
                
                genre_avg_ratings.append({
                    'Genre': genre,
                    'Average_Rating': avg_rating,
                    'Game_Count': game_count
                })
        
        ratings_summary = pd.DataFrame(genre_avg_ratings)
        ratings_summary = ratings_summary.sort_values('Average_Rating', ascending=False)
        
        logger.info("Resumen de ratings calculado")
        return ratings_summary
    
    def get_summary_statistics(self):
        """
        Genera estadísticas generales del dataset
        
        Returns:
            dict: Diccionario con estadísticas
        """
        stats = {
            'total_games': len(self.df),
            'total_plays': self.df['Plays_numeric'].sum(),
            'average_rating': self.df['Rating'].mean(),
            'max_rating': self.df['Rating'].max(),
            'min_rating': self.df['Rating'].min(),
            'unique_genres': len(set([g for genres in self.df['Genres_list'] for g in genres]))
        }
        return stats
