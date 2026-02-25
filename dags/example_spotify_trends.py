"""
## Spotify Trends - Análisis de tendencias musicales

Este DAG simula un pipeline de análisis de tendencias musicales al estilo Spotify.
Está diseñado para enseñar los conceptos fundamentales de Airflow de forma práctica.

### Conceptos que aprenderás con este DAG:

1. **TaskFlow API (@task)**: Cómo convertir funciones Python en tareas de Airflow.
2. **Dependencias entre tareas**: Cómo una tarea pasa datos a la siguiente (XCom).
3. **Branching**: Cómo tomar decisiones y ejecutar caminos distintos según los datos.
4. **Tareas en paralelo**: Cómo ejecutar varias tareas al mismo tiempo.
5. **Trigger rules**: Cómo controlar cuándo se ejecuta una tarea según el estado de las anteriores.

### Flujo del pipeline:

```
  fetch_top_songs
        |
  analyze_genres
      /    \\
 [branch]   \\
   /   \\     \\
 pop  rock   generate_playlist
   \\   /         |
  join_results ---+
        |
   send_report
```

No requiere credenciales ni APIs externas. Usa datos simulados.

### Documentación oficial de Airflow

- [Branching](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#branchpythonoperator)
- [Trigger rules](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules)
- [Dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
"""

import logging
import random
from collections import Counter

from airflow.operators.python import BranchPythonOperator
from airflow.sdk import dag, task
from pendulum import datetime, duration

# --------------- #
# DAG Constants   #
# --------------- #

STREAM_VARIATION_MIN = -500_000
STREAM_VARIATION_MAX = 500_000
PLAYLIST_SIZE = 5

MAINSTREAM_GENRES = ("Pop", "Hip-Hop", "R&B")

TASK_ID_ANALYZE_GENRES = "analyze_genres"
TASK_ID_DEEP_DIVE_POP = "deep_dive_pop"
TASK_ID_DEEP_DIVE_ROCK = "deep_dive_rock"
TASK_ID_GENERATE_PLAYLIST = "generate_playlist"

MOCK_SONGS: list[dict] = [
    {"title": "Flowers", "artist": "Miley Cyrus", "genre": "Pop", "streams": 2_800_000},
    {"title": "Vampire", "artist": "Olivia Rodrigo", "genre": "Pop", "streams": 2_100_000},
    {"title": "Cruel Summer", "artist": "Taylor Swift", "genre": "Pop", "streams": 3_500_000},
    {"title": "Paint The Town Red", "artist": "Doja Cat", "genre": "Hip-Hop", "streams": 1_900_000},
    {"title": "Snooze", "artist": "SZA", "genre": "R&B", "streams": 1_700_000},
    {"title": "Greedy", "artist": "Tate McRae", "genre": "Pop", "streams": 2_400_000},
    {"title": "Barbie World", "artist": "Nicki Minaj", "genre": "Hip-Hop", "streams": 1_600_000},
    {"title": "Last Night", "artist": "Morgan Wallen", "genre": "Country", "streams": 2_000_000},
    {"title": "Kill Bill", "artist": "SZA", "genre": "R&B", "streams": 2_600_000},
    {"title": "The Emptiness Machine", "artist": "Linkin Park", "genre": "Rock", "streams": 1_500_000},
    {"title": "Lux Aeterna", "artist": "Metallica", "genre": "Rock", "streams": 1_200_000},
    {"title": "Running Up That Hill", "artist": "Kate Bush", "genre": "Rock", "streams": 1_800_000},
]

log = logging.getLogger(__name__)


# --------------- #
# DAG Definition  #
# --------------- #


@dag(
    start_date=datetime(2025, 4, 1),
    schedule="@weekly",
    max_consecutive_failed_dag_runs=3,
    doc_md=__doc__,
    default_args={
        "owner": "Universidad",
        "retries": 2,
        "retry_delay": duration(seconds=5),
    },
    tags=["example", "educativo", "spotify", "branching"],
    is_paused_upon_creation=True,  # El DAG se crea pausado; hay que activarlo manualmente en la UI
    catchup=False,  # No ejecuta runs pasados; solo programa desde ahora en adelante
)
def example_spotify_trends():
    """Pipeline de análisis de tendencias musicales con branching y paralelismo."""

    # =========================================================
    # CONCEPTO 1: Tareas básicas con @task (TaskFlow API)
    # =========================================================

    @task
    def fetch_top_songs() -> list[dict]:
        """
        EXTRACT: Simula la obtención de datos de una API de música.
        En un caso real, aquí llamarías a la API de Spotify.
        """
        randomized_songs: list[dict] = []
        for song in MOCK_SONGS:
            stream_variation = random.randint(STREAM_VARIATION_MIN, STREAM_VARIATION_MAX)
            base_streams = song.get("streams", 0)
            adjusted_streams = max(0, base_streams + stream_variation)
            randomized_songs.append({**song, "streams": adjusted_streams})

        randomized_songs.sort(key=lambda s: s.get("streams", 0), reverse=True)

        log.info("Se obtuvieron %d canciones del top chart.", len(randomized_songs))
        for rank, song in enumerate(randomized_songs[:PLAYLIST_SIZE], start=1):
            log.info(
                "  #%d %s - %s (%s streams)",
                rank, song.get("title", "?"), song.get("artist", "?"),
                f"{song.get('streams', 0):,}",
            )
        return randomized_songs

    @task
    def analyze_genres(songs: list[dict]) -> dict:
        """
        TRANSFORM: Analiza los géneros más populares.
        Cuenta streams totales por género y determina el dominante.
        """
        streams_by_genre: dict[str, int] = {}
        count_by_genre: dict[str, int] = Counter()

        for song in songs:
            genre: str = song.get("genre", "Unknown")
            streams_by_genre[genre] = streams_by_genre.get(genre, 0) + song.get("streams", 0)
            count_by_genre[genre] += 1

        dominant_genre: str = max(streams_by_genre, key=streams_by_genre.get)

        genre_analysis = {
            "streams_by_genre": streams_by_genre,
            "count_by_genre": dict(count_by_genre),
            "dominant_genre": dominant_genre,
            "total_streams": sum(streams_by_genre.values()),
        }

        log.info("Género dominante: %s", dominant_genre)
        for genre, streams in sorted(streams_by_genre.items(), key=lambda x: -x[1]):
            log.info(
                "  %s: %s streams (%d canciones)",
                genre, f"{streams:,}", count_by_genre[genre],
            )
        return genre_analysis

    # =========================================================
    # CONCEPTO 2: Branching (decisiones en el pipeline)
    # =========================================================

    def _choose_trend_path(**context) -> str:
        """Decide qué análisis detallado ejecutar según el género dominante."""
        task_instance = context["ti"]
        genre_analysis: dict = task_instance.xcom_pull(task_ids=TASK_ID_ANALYZE_GENRES)
        dominant_genre: str = genre_analysis.get("dominant_genre", "Unknown")

        if dominant_genre in MAINSTREAM_GENRES:
            log.info("Género '%s' → camino Pop/mainstream", dominant_genre)
            return TASK_ID_DEEP_DIVE_POP

        log.info("Género '%s' → camino Rock/alternativo", dominant_genre)
        return TASK_ID_DEEP_DIVE_ROCK

    branch_operator = BranchPythonOperator(
        task_id="choose_trend_path",
        python_callable=_choose_trend_path,
    )

    @task
    def deep_dive_pop(**context) -> str:
        """Análisis detallado cuando domina el pop/mainstream."""
        task_instance = context["ti"]
        genre_analysis: dict = task_instance.xcom_pull(task_ids=TASK_ID_ANALYZE_GENRES)
        streams_by_genre: dict = genre_analysis.get("streams_by_genre", {})
        pop_streams: int = streams_by_genre.get("Pop", 0)

        insight_message = (
            f"Tendencia POP dominante con {pop_streams:,} streams en Pop. "
            f"Las playlists de 'Today's Top Hits' están en su pico."
        )
        log.info(insight_message)
        return insight_message

    @task
    def deep_dive_rock(**context) -> str:
        """Análisis detallado cuando domina el rock/alternativo."""
        task_instance = context["ti"]
        genre_analysis: dict = task_instance.xcom_pull(task_ids=TASK_ID_ANALYZE_GENRES)
        streams_by_genre: dict = genre_analysis.get("streams_by_genre", {})
        rock_streams: int = streams_by_genre.get("Rock", 0)

        insight_message = (
            f"Tendencia ROCK dominante con {rock_streams:,} streams. "
            f"El rock está de vuelta en las playlists principales."
        )
        log.info(insight_message)
        return insight_message

    # =========================================================
    # CONCEPTO 3: Tareas en paralelo
    # =========================================================

    @task
    def generate_playlist(songs: list[dict]) -> list[str]:
        """Genera una playlist con las top 5 canciones (en paralelo al branching)."""
        top_songs = songs[:PLAYLIST_SIZE]
        playlist_entries: list[str] = [
            f"{song.get('artist', '?')} - {song.get('title', '?')}" for song in top_songs
        ]

        log.info("Playlist generada:")
        for position, track in enumerate(playlist_entries, start=1):
            log.info("  %d. %s", position, track)
        return playlist_entries

    # =========================================================
    # CONCEPTO 4: Trigger rules (join después de branch)
    # =========================================================

    @task(trigger_rule="none_failed_min_one_success")
    def send_weekly_report(**context) -> None:
        """
        LOAD: Genera el reporte final combinando todos los resultados.
        Se ejecuta sin importar qué camino del branch se tomó.
        """
        task_instance = context["ti"]
        genre_analysis: dict = task_instance.xcom_pull(task_ids=TASK_ID_ANALYZE_GENRES)
        playlist: list[str] = task_instance.xcom_pull(task_ids=TASK_ID_GENERATE_PLAYLIST) or []

        pop_insight: str | None = task_instance.xcom_pull(task_ids=TASK_ID_DEEP_DIVE_POP)
        rock_insight: str | None = task_instance.xcom_pull(task_ids=TASK_ID_DEEP_DIVE_ROCK)
        trend_insight: str = pop_insight or rock_insight or "Sin datos"

        log.info("=" * 50)
        log.info("REPORTE SEMANAL DE TENDENCIAS MUSICALES")
        log.info("=" * 50)
        log.info("Total streams analizados: %s", f"{genre_analysis.get('total_streams', 0):,}")
        log.info("Género dominante: %s", genre_analysis.get("dominant_genre", "N/A"))
        log.info("Insight: %s", trend_insight)
        log.info("Top playlist: %s...", ", ".join(playlist[:3]))
        log.info("=" * 50)

    # =========================================================
    # Dependencias entre tareas
    # =========================================================

    all_songs = fetch_top_songs()
    genre_analysis = analyze_genres(all_songs)

    # El branch depende del análisis
    branch_operator.set_upstream(genre_analysis)

    # Dos caminos posibles después del branch
    pop_analysis = deep_dive_pop()
    rock_analysis = deep_dive_rock()
    branch_operator >> [pop_analysis, rock_analysis]

    # La playlist se genera en paralelo (depende de songs, no del branch)
    playlist = generate_playlist(all_songs)

    # El reporte final espera a todo: ambos branches + playlist
    weekly_report = send_weekly_report()
    pop_analysis >> weekly_report
    rock_analysis >> weekly_report
    playlist >> weekly_report


example_spotify_trends()
