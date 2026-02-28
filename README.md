# VGS - ETL de Análisis de Videojuegos 🎮

Pipeline ETL (Extract, Transform, Load) para análisis de datos de videojuegos del dataset de Backloggd.

## 📋 Descripción

Este proyecto implementa un pipeline ETL completo que:
- **Extrae** datos desde un archivo CSV con información de videojuegos
- **Transforma** y analiza los datos para obtener insights valiosos
- **Carga** y genera visualizaciones gráficas y reportes textuales

## 🎯 Análisis Implementados

### 1. Juego Más Jugado
Identifica y presenta el videojuego con mayor número de jugadas en la plataforma.

### 2. Top 20 Géneros Más Jugados
Analiza y visualiza los 20 géneros de videojuegos más populares basándose en el total de jugadas acumuladas.

### 3. Mejores Valorados por Género
Calcula los ratings promedio por género y presenta los géneros mejor valorados mediante una gráfica de torta.

### 4. Análisis Combinado
Visualización que relaciona la popularidad (jugadas) con la calidad (rating) de los géneros.

## 🏗️ Estructura del Proyecto

```
VGS/
├── backloggd_games.csv          # Dataset fuente
├── main.py                       # Script principal del ETL
├── requirements.txt              # Dependencias Python
├── README.md                     # Este archivo
├── etl_execution.log            # Log de ejecución (generado)
│
├── extract/
│   └── extract.py               # Módulo de extracción
│
├── tranform/
│   └── transform.py             # Módulo de transformación
│
├── load/
│   └── load.py                  # Módulo de carga y visualización
│
└── output/                      # Resultados (generado)
    ├── analisis_videojuegos.txt
    ├── juego_mas_jugado.png
    ├── top_20_generos.png
    ├── ratings_por_genero_torta.png
    └── analisis_combinado.png
```

## 🚀 Instalación

### Prerrequisitos
- Python 3.8 o superior
- pip (gestor de paquetes de Python)

### Pasos

1. **Clonar el repositorio** (si aplica)
```bash
git clone <url-repositorio>
cd VGS
```

2. **Crear entorno virtual** (recomendado)
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

3. **Instalar dependencias**
```bash
pip install -r requirements.txt
```

## 💻 Uso

### Ejecución Simple
```bash
python main.py
```

### Salida Esperada
El script ejecutará las tres fases del ETL y generará:
- **Reporte textual** con estadísticas detalladas
- **4 gráficos** en formato PNG de alta resolución
- **Log de ejecución** con información detallada del proceso

### Resultados
Todos los resultados se guardan en la carpeta `output/`:

| Archivo | Descripción |
|---------|-------------|
| `analisis_videojuegos.txt` | Reporte textual completo con todas las estadísticas |
| `juego_mas_jugado.png` | Gráfico destacando el juego más jugado |
| `top_20_generos.png` | Gráfico de barras con los 20 géneros más jugados |
| `ratings_por_genero_torta.png` | Gráfico de torta con ratings por género |
| `analisis_combinado.png` | Análisis multidimensional de géneros |

## 📊 Estructura de Datos

El dataset CSV contiene las siguientes columnas:
- `Title`: Nombre del juego
- `Release_Date`: Fecha de lanzamiento
- `Developers`: Desarrolladores del juego
- `Summary`: Descripción del juego
- `Platforms`: Plataformas disponibles
- `Genres`: Géneros del juego (lista)
- `Rating`: Valoración promedio (0-5)
- `Plays`: Número de jugadas
- `Playing`: Jugadores actuales
- `Backlogs`: Juegos pendientes
- `Wishlist`: Lista de deseos
- `Lists`: Listas que incluyen el juego
- `Reviews`: Número de reseñas

## 🔧 Módulos del ETL

### Extract (`extract/extract.py`)
- Lectura del archivo CSV
- Validación de datos
- Información del dataset

### Transform (`tranform/transform.py`)
- Limpieza y normalización de datos
- Conversión de formatos (jugadas, géneros)
- Cálculos de métricas:
  - Juego más jugado
  - Top géneros por jugadas
  - Ratings promedio por género
  - Estadísticas generales

### Load (`load/load.py`)
- Generación de reportes textuales
- Creación de visualizaciones:
  - Gráficos de barras
  - Gráficos de torta
  - Análisis combinados
- Exportación de resultados

## 📈 Tipos de Visualizaciones

### 1. Gráfico de Barra Simple
Destaca el juego más jugado con su número de jugadas y rating.

### 2. Gráfico de Barras Horizontales
Top 20 géneros ordenados por total de jugadas, con gradiente de colores.

### 3. Gráfico de Torta + Barras
Combina visualización circular de distribución de ratings con comparación de barras.

### 4. Gráfico Combinado (Dual Axis)
Relaciona cantidad de jugadas (barras) con calidad promedio (línea) por género.

## 🛠️ Tecnologías Utilizadas

- **Python 3.8+**: Lenguaje principal
- **Pandas**: Manipulación y análisis de datos
- **Matplotlib**: Generación de gráficos
- **Seaborn**: Visualizaciones estadísticas mejoradas
- **NumPy**: Operaciones numéricas

## 📝 Logging

El sistema genera logs detallados en `etl_execution.log` que incluyen:
- Inicio/fin de cada fase
- Registros procesados
- Errores y advertencias
- Tiempo de ejecución

## ⚙️ Personalización

### Cambiar número de géneros analizados
En `main.py`, modifica el parámetro `top_n`:
```python
genres_df = transformer.get_top_genres(top_n=30)  # Default: 20
```

### Cambiar directorio de salida
En `main.py`, modifica el parámetro `output_dir`:
```python
loader = DataLoader(output_dir='mis_resultados')  # Default: 'output'
```

### Personalizar visualizaciones
Edita los métodos en `load/load.py` para ajustar:
- Colores
- Tamaños de figura
- Estilos de gráfico
- Resolución DPI

## 🐛 Troubleshooting

### Error: "ModuleNotFoundError"
Asegúrate de haber instalado las dependencias:
```bash
pip install -r requirements.txt
```

### Error: "FileNotFoundError: backloggd_games.csv"
Verifica que el archivo CSV esté en la raíz del proyecto.

### Gráficos no se generan
Revisa que la carpeta `output/` tenga permisos de escritura.

## 📄 Licencia

Este proyecto es de código abierto y está disponible bajo la licencia MIT.

## 👥 Contribuciones

Las contribuciones son bienvenidas. Por favor:
1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

## 📧 Contacto

Para preguntas o sugerencias, por favor abre un issue en el repositorio.

---

**Desarrollado con ❤️ para el análisis de datos de videojuegos**