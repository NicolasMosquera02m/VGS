"""
extract.py - Módulo de Extracción del ETL
Extrae datos del archivo CSV de videojuegos
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataExtractor:
    """Clase para extraer datos del CSV"""
    
    def __init__(self, file_path):
        """
        Inicializa el extractor con la ruta del archivo
        
        Args:
            file_path (str): Ruta al archivo CSV
        """
        self.file_path = file_path
        
    def extract_data(self):
        """
        Extrae datos del CSV y devuelve un DataFrame
        
        Returns:
            pd.DataFrame: DataFrame con los datos extraídos
        """
        try:
            logger.info(f"Extrayendo datos desde: {self.file_path}")
            df = pd.read_csv(self.file_path, index_col=0)
            logger.info(f"Datos extraídos exitosamente. Registros: {len(df)}")
            logger.info(f"Columnas: {list(df.columns)}")
            return df
        except FileNotFoundError:
            logger.error(f"Archivo no encontrado: {self.file_path}")
            raise
        except Exception as e:
            logger.error(f"Error al extraer datos: {str(e)}")
            raise
    
    def get_data_info(self, df):
        """
        Obtiene información básica del DataFrame
        
        Args:
            df (pd.DataFrame): DataFrame a analizar
            
        Returns:
            dict: Diccionario con información del DataFrame
        """
        info = {
            'total_records': len(df),
            'columns': list(df.columns),
            'missing_values': df.isnull().sum().to_dict(),
            'data_types': df.dtypes.to_dict()
        }
        return info
