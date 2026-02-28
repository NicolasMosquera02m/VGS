"""
load.py - Módulo de Carga del ETL
Genera visualizaciones y reportes de los datos transformados
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import logging
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurar estilo de gráficos
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10


class DataLoader:
    """Clase para cargar datos y generar visualizaciones"""
    
    def __init__(self, output_dir='output'):
        """
        Inicializa el loader con un directorio de salida
        
        Args:
            output_dir (str): Directorio donde guardar los outputs
        """
        self.output_dir = output_dir
        self._create_output_dir()
        
    def _create_output_dir(self):
        """Crea el directorio de salida si no existe"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Directorio de salida creado: {self.output_dir}")
    
    def generate_text_report(self, most_played, stats, genres_df, ratings_summary):
        """
        Genera un reporte textual del análisis
        
        Args:
            most_played (dict): Información del juego más jugado
            stats (dict): Estadísticas generales
            genres_df (pd.DataFrame): DataFrame con géneros más jugados
            ratings_summary (pd.DataFrame): DataFrame con ratings por género
        """
        logger.info("Generando reporte textual...")
        
        report_path = os.path.join(self.output_dir, 'analisis_videojuegos.txt')
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("ANÁLISIS DE VIDEOJUEGOS - BACKLOGGD\n")
            f.write("=" * 80 + "\n")
            f.write(f"Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Estadísticas generales
            f.write("ESTADÍSTICAS GENERALES\n")
            f.write("-" * 80 + "\n")
            f.write(f"Total de juegos: {stats['total_games']:,}\n")
            f.write(f"Total de jugadas: {stats['total_plays']:,}\n")
            f.write(f"Rating promedio: {stats['average_rating']:.2f}\n")
            f.write(f"Rating máximo: {stats['max_rating']:.2f}\n")
            f.write(f"Rating mínimo: {stats['min_rating']:.2f}\n")
            f.write(f"Géneros únicos: {stats['unique_genres']}\n\n")
            
            # Juego más jugado
            f.write("JUEGO MÁS JUGADO\n")
            f.write("-" * 80 + "\n")
            f.write(f"Título: {most_played['title']}\n")
            f.write(f"Número de jugadas: {most_played['plays']}\n")
            f.write(f"Rating: {most_played['rating']:.2f}\n")
            f.write(f"Géneros: {most_played['genres']}\n")
            f.write(f"Plataformas: {most_played['platforms']}\n")
            f.write(f"Fecha de lanzamiento: {most_played['release_date']}\n\n")
            
            # Top 20 géneros más jugados
            f.write("TOP 20 GÉNEROS MÁS JUGADOS\n")
            f.write("-" * 80 + "\n")
            for idx, row in genres_df.iterrows():
                f.write(f"{idx+1}. {row['Genre']}: {row['Total_Plays']:,} jugadas\n")
            f.write("\n")
            
            # Ratings promedio por género
            f.write("RATINGS PROMEDIO POR GÉNERO (TOP 20 GÉNEROS)\n")
            f.write("-" * 80 + "\n")
            for idx, row in ratings_summary.iterrows():
                f.write(f"{row['Genre']}: {row['Average_Rating']:.2f} ")
                f.write(f"({row['Game_Count']} juegos)\n")
            f.write("\n")
            
            f.write("=" * 80 + "\n")
            f.write("Fin del reporte\n")
            f.write("=" * 80 + "\n")
        
        logger.info(f"Reporte textual guardado en: {report_path}")
        return report_path
    
    def plot_most_played_game(self, most_played):
        """
        Genera una visualización destacada del juego más jugado
        
        Args:
            most_played (dict): Información del juego más jugado
        """
        logger.info("Generando gráfico del juego más jugado...")
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Crear un gráfico de barra simple para destacar el juego
        ax.barh([0], [most_played['plays_numeric']], color='#1f77b4', height=0.5)
        
        # Configurar etiquetas
        ax.set_yticks([0])
        ax.set_yticklabels([most_played['title']], fontsize=12, fontweight='bold')
        ax.set_xlabel('Número de Jugadas', fontsize=12)
        ax.set_title('JUEGO MÁS JUGADO', fontsize=16, fontweight='bold', pad=20)
        
        # Añadir valor en la barra
        ax.text(most_played['plays_numeric'], 0, 
                f" {most_played['plays']} jugadas\n Rating: {most_played['rating']:.1f}/5.0", 
                va='center', ha='left', fontsize=11, fontweight='bold')
        
        # Formato del eje x
        ax.ticklabel_format(style='plain', axis='x')
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        
        plt.tight_layout()
        
        output_path = os.path.join(self.output_dir, 'juego_mas_jugado.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Gráfico guardado en: {output_path}")
        return output_path
    
    def plot_top_genres(self, genres_df):
        """
        Genera gráfico de barras con los top 20 géneros más jugados
        
        Args:
            genres_df (pd.DataFrame): DataFrame con géneros y jugadas
        """
        logger.info("Generando gráfico de top géneros...")
        
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Crear gráfico de barras horizontal
        colors = plt.cm.viridis(range(len(genres_df)))
        bars = ax.barh(range(len(genres_df)), genres_df['Total_Plays'], color=colors)
        
        # Configurar etiquetas
        ax.set_yticks(range(len(genres_df)))
        ax.set_yticklabels(genres_df['Genre'], fontsize=11)
        ax.set_xlabel('Total de Jugadas', fontsize=12, fontweight='bold')
        ax.set_title('TOP 20 GÉNEROS MÁS JUGADOS', fontsize=16, fontweight='bold', pad=20)
        
        # Añadir valores en las barras
        for i, (idx, row) in enumerate(genres_df.iterrows()):
            ax.text(row['Total_Plays'], i, f" {row['Total_Plays']:,.0f}", 
                   va='center', ha='left', fontsize=9)
        
        # Formato del eje x
        ax.ticklabel_format(style='plain', axis='x')
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x/1000)}K'))
        
        # Invertir para que el más jugado esté arriba
        ax.invert_yaxis()
        
        plt.tight_layout()
        
        output_path = os.path.join(self.output_dir, 'top_20_generos.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Gráfico guardado en: {output_path}")
        return output_path
    
    def plot_genre_ratings_pie(self, ratings_summary, top_n=15):
        """
        Genera gráfico de torta con ratings promedio por género
        
        Args:
            ratings_summary (pd.DataFrame): DataFrame con ratings por género
            top_n (int): Número de géneros a mostrar
        """
        logger.info("Generando gráfico de torta de ratings...")
        
        # Tomar solo los top N mejor valorados
        top_ratings = ratings_summary.head(top_n).copy()
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 9))
        
        # Gráfico de torta
        colors = plt.cm.Set3(range(len(top_ratings)))
        wedges, texts, autotexts = ax1.pie(
            top_ratings['Average_Rating'], 
            labels=top_ratings['Genre'],
            autopct='%1.1f%%',
            startangle=90,
            colors=colors,
            textprops={'fontsize': 9}
        )
        
        ax1.set_title(f'TOP {top_n} GÉNEROS MEJOR VALORADOS\n(Distribución de Ratings Promedio)', 
                     fontsize=14, fontweight='bold', pad=20)
        
        # Gráfico de barras complementario
        bars = ax2.barh(range(len(top_ratings)), top_ratings['Average_Rating'], 
                       color=colors, edgecolor='black', linewidth=0.5)
        ax2.set_yticks(range(len(top_ratings)))
        ax2.set_yticklabels(top_ratings['Genre'], fontsize=10)
        ax2.set_xlabel('Rating Promedio', fontsize=11, fontweight='bold')
        ax2.set_title(f'Ratings Promedio por Género (Top {top_n})', 
                     fontsize=12, fontweight='bold', pad=15)
        ax2.set_xlim(0, 5)
        ax2.grid(axis='x', alpha=0.3)
        
        # Añadir valores en las barras
        for i, (idx, row) in enumerate(top_ratings.iterrows()):
            ax2.text(row['Average_Rating'], i, f" {row['Average_Rating']:.2f}", 
                    va='center', ha='left', fontsize=9, fontweight='bold')
        
        # Invertir para que el mejor esté arriba
        ax2.invert_yaxis()
        
        plt.tight_layout()
        
        output_path = os.path.join(self.output_dir, 'ratings_por_genero_torta.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Gráfico guardado en: {output_path}")
        return output_path
    
    def plot_comprehensive_analysis(self, genres_df, ratings_summary):
        """
        Genera un gráfico combinado de géneros jugados vs ratings
        
        Args:
            genres_df (pd.DataFrame): DataFrame con géneros más jugados
            ratings_summary (pd.DataFrame): DataFrame con ratings por género
        """
        logger.info("Generando análisis combinado...")
        
        # Combinar datos
        combined = pd.merge(genres_df, ratings_summary, on='Genre', how='inner')
        combined = combined.head(15)  # Top 15 para mejor visualización
        
        fig, ax1 = plt.subplots(figsize=(14, 8))
        
        # Eje 1: Total de jugadas (barras)
        color1 = '#3498db'
        x = range(len(combined))
        bars = ax1.bar(x, combined['Total_Plays'], color=color1, alpha=0.7, label='Total Jugadas')
        ax1.set_xlabel('Género', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Total de Jugadas', color=color1, fontsize=12, fontweight='bold')
        ax1.tick_params(axis='y', labelcolor=color1)
        ax1.set_xticks(x)
        ax1.set_xticklabels(combined['Genre'], rotation=45, ha='right', fontsize=10)
        
        # Eje 2: Rating promedio (línea)
        ax2 = ax1.twinx()
        color2 = '#e74c3c'
        line = ax2.plot(x, combined['Average_Rating'], color=color2, marker='o', 
                       linewidth=2, markersize=8, label='Rating Promedio')
        ax2.set_ylabel('Rating Promedio', color=color2, fontsize=12, fontweight='bold')
        ax2.tick_params(axis='y', labelcolor=color2)
        ax2.set_ylim(0, 5)
        
        # Título
        plt.title('ANÁLISIS COMBINADO: GÉNEROS MÁS JUGADOS VS MEJOR VALORADOS', 
                 fontsize=14, fontweight='bold', pad=20)
        
        # Leyenda combinada
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
        
        plt.tight_layout()
        
        output_path = os.path.join(self.output_dir, 'analisis_combinado.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Gráfico guardado en: {output_path}")
        return output_path
    
    def generate_all_visualizations(self, most_played, stats, genres_df, ratings_summary):
        """
        Genera todas las visualizaciones y reportes
        
        Args:
            most_played (dict): Información del juego más jugado
            stats (dict): Estadísticas generales
            genres_df (pd.DataFrame): DataFrame con géneros más jugados
            ratings_summary (pd.DataFrame): DataFrame con ratings por género
            
        Returns:
            dict: Diccionario con rutas de los archivos generados
        """
        logger.info("Generando todas las visualizaciones...")
        
        outputs = {
            'text_report': self.generate_text_report(most_played, stats, genres_df, ratings_summary),
            'most_played_chart': self.plot_most_played_game(most_played),
            'top_genres_chart': self.plot_top_genres(genres_df),
            'ratings_pie_chart': self.plot_genre_ratings_pie(ratings_summary),
            'combined_analysis': self.plot_comprehensive_analysis(genres_df, ratings_summary)
        }
        
        logger.info("Todas las visualizaciones generadas exitosamente")
        return outputs
