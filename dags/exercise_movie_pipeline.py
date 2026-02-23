"""
## Ejercicio: Pipeline de películas 🎬

Pipeline incompleto para practicar
Usa la API gratuita de OMDb (https://www.omdbapi.com/) para buscar películas.

### Instrucciones

1. Obtén una API key gratuita en: https://www.omdbapi.com/apikey.aspx
2. Reemplaza "TU_API_KEY" por tu key real.
3. Completa las funciones marcadas con TODO.
4. Ejecuta el DAG y verifica que funciona en la UI de Airflow.

### Flujo del pipeline:

```
  fetch_movie_data  →  transform_movie_info  →  display_results
```

### Conceptos que practicarás:
- Llamadas a APIs externas con requests
- Transformación de datos con diccionarios
- Paso de datos entre tareas con XCom (TaskFlow API)
- Acceso seguro a diccionarios con .get()
- Logging con el módulo logging
"""

import logging

import requests
from airflow.sdk import dag, task
from pendulum import datetime, duration

# --------------- #
# DAG Constants   #
# --------------- #

OMDB_API_URL = "https://www.omdbapi.com/"
OMDB_API_KEY = "TU_API_KEY"  # ⚠️ Reemplaza con tu API key gratuita
HTTP_TIMEOUT_SECONDS = 15

# Lista de películas a buscar
# Nota: la API de OMDb usa títulos en inglés.
MOVIES_TO_SEARCH: list[str] = [
    "Wuthering Heights",
    "Ídolos",
    "Oppenheimer",
    "Inside Out 2",
    "Dune: Part Two",
    "The Wild Robot",
    "Everything Everywhere All at Once",
    "Emilia Pérez",
]

log = logging.getLogger(__name__)


# --------------- #
# DAG Definition  #
# --------------- #


@dag(
    start_date=datetime(2025, 4, 1),
    schedule=None,  # Solo ejecución manual
    doc_md=__doc__,
    default_args={
        "owner": "Universidad",
        "retries": 2,
        "retry_delay": duration(seconds=5),
    },
    tags=["ejercicio", "educativo", "peliculas"],
    is_paused_upon_creation=True,  # El DAG se crea pausado; hay que activarlo manualmente en la UI
    catchup=False,  # No ejecuta runs pasados; solo programa desde ahora en adelante
)
def exercise_movie_pipeline():
    """Pipeline de películas para completar"""

    @task
    def fetch_movie_data() -> list[dict]:
        """
        Busca información de cada película en la API de OMDb.

        TODO 1: Completa la llamada a la API.
        - Usa requests.get() con los parámetros: t=titulo, apikey=OMDB_API_KEY
        - Verifica que la respuesta sea exitosa con response.raise_for_status()
        - Añade el JSON de la respuesta a la lista de resultados
        - Usa log.info() para registrar cada película encontrada

        Ejemplo de URL: https://www.omdbapi.com/?t=Inception&apikey=TU_KEY
        """
        movie_results: list[dict] = []

        for movie_title in MOVIES_TO_SEARCH:
            # TODO 1: Haz la llamada a la API aquí
            # response = requests.get(...)
            # movie_data = response.json()
            # movie_results.append(movie_data)
            pass

        log.info("Se obtuvieron %d películas.", len(movie_results))
        return movie_results

    @task
    def transform_movie_info(raw_movies: list[dict]) -> list[dict]:
        """
        Transforma los datos crudos de la API en un formato limpio.

        TODO 2: Extrae los campos relevantes de cada película.
        Para cada película en raw_movies, crea un diccionario con:
        - "title": título (campo "Title" de la API)
        - "year": año (campo "Year")
        - "director": director (campo "Director")
        - "rating_imdb": puntuación IMDb (campo "imdbRating"), convertida a float
        - "genre": género (campo "Genre")
        - "plot": sinopsis corta (campo "Plot")

        """
        clean_movies: list[dict] = []

        for movie in raw_movies:
            # TODO 2: Transforma cada película aquí
            # clean_movie = {
            #     "title": movie.get("Title", "Desconocido"),
            #     ...
            # }
            # clean_movies.append(clean_movie)
            pass

        log.info("Se transformaron %d películas.", len(clean_movies))
        return clean_movies

    @task
    def display_results(movies: list[dict]) -> None:
        """
        Muestra los resultados del análisis.

        TODO 3: Implementa la lógica para mostrar:
        - Un listado de todas las películas con su rating
        - La película con mejor puntuación IMDb
        - El promedio de puntuación de todas las películas

        Usa log.info() en vez de print().
        """
        if not movies:
            log.warning("No hay películas para mostrar.")
            return

        # TODO 3: Muestra los resultados aquí
        # for movie in movies:
        #     log.info(...)
        #
        # best_movie = max(movies, key=lambda m: m.get("rating_imdb", 0))
        # log.info("Mejor película: %s", best_movie.get("title"))
        pass

    # --- Dependencias ---
    # Con TaskFlow API (@task), las dependencias se crean automáticamente
    # al pasar el return de una tarea como parámetro de la siguiente.
    # Además, los datos se mueven entre tareas vía XCom sin código extra.
    #
    # Esto equivale a: fetch >> transform >> display
    # pero con paso de datos incluido. La sintaxis ">>" solo define orden
    # de ejecución sin mover datos (se usa con operadores tradicionales).
    raw_data = fetch_movie_data()
    clean_data = transform_movie_info(raw_data)
    display_results(clean_data)


exercise_movie_pipeline()
