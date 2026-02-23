"""
## Spotify Top 5 - Ejemplo de XCom

Este DAG enseña cómo funciona XCom en Airflow: el mecanismo que permite
que las tareas compartan datos entre sí.

### ¿Qué es XCom?

XCom (cross-communication) es como una "memoria compartida" entre tareas.
Cuando una tarea retorna un valor, Airflow lo guarda automáticamente.
La siguiente tarea puede leerlo y usarlo.

### Flujo:

```
fetch_songs  →  rank_top_songs  →  show_daily_summary
```

- **fetch_songs**: Genera una lista de canciones con streams aleatorios.
  El `return` guarda los datos en XCom automáticamente.
- **rank_top_songs**: Recibe esos datos (Airflow los inyecta como parámetro),
  ordena las canciones y devuelve el top 5.
- **show_daily_summary**: Recibe el top 5 y lo imprime.

No requiere APIs ni credenciales. Solo Python puro.

### Documentación oficial de Airflow

- [XCom](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Conceptos: Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)
"""

import logging
import random

from airflow.sdk import dag, task
from pendulum import datetime, duration

# --------------- #
# DAG Constants   #
# --------------- #

TOP_N = 5
MIN_STREAMS = 500_000
MAX_STREAMS = 5_000_000

SAMPLE_CATALOG: list[dict[str, str]] = [
    {"title": "Espresso", "artist": "Sabrina Carpenter"},
    {"title": "Birds of a Feather", "artist": "Billie Eilish"},
    {"title": "Taste", "artist": "Sabrina Carpenter"},
    {"title": "Die With A Smile", "artist": "Lady Gaga & Bruno Mars"},
    {"title": "APT.", "artist": "ROSÉ & Bruno Mars"},
    {"title": "That's So True", "artist": "Gracie Abrams"},
    {"title": "Sympathy Is a Knife", "artist": "Charli XCX"},
    {"title": "Good Luck Babe", "artist": "Chappell Roan"},
    {"title": "Guess", "artist": "Charli XCX & Billie Eilish"},
    {"title": "Lunch", "artist": "Billie Eilish"},
]

log = logging.getLogger(__name__)


# --------------- #
# DAG Definition  #
# --------------- #


@dag(
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    max_consecutive_failed_dag_runs=3,
    doc_md=__doc__,
    default_args={
        "owner": "Universidad",
        "retries": 2,
        "retry_delay": duration(seconds=5),
    },
    tags=["example", "educativo", "spotify", "xcom"],
    is_paused_upon_creation=True,  # El DAG se crea pausado; hay que activarlo manualmente en la UI
    catchup=False,  # No ejecuta runs pasados; solo programa desde ahora en adelante
)
def example_spotify_xcom():
    """Pipeline lineal que demuestra XCom: fetch → rank → summary."""

    @task
    def fetch_songs() -> list[dict]:
        """
        Simula obtener canciones de una API.

        XCOM: Al hacer ``return``, Airflow guarda este valor
        automáticamente para que otras tareas lo lean.
        Puedes verlo en la UI: click en la tarea → XCom.
        """
        songs_with_streams: list[dict] = [
            {**song, "streams": random.randint(MIN_STREAMS, MAX_STREAMS)}
            for song in SAMPLE_CATALOG
        ]
        log.info("Obtenidas %d canciones del catálogo.", len(songs_with_streams))
        return songs_with_streams  # ← Esto se guarda en XCom

    @task
    def rank_top_songs(all_songs: list[dict]) -> list[dict]:
        """
        Ordena las canciones por streams y devuelve el top 5.

        XCOM: El parámetro ``all_songs`` viene automáticamente de
        la tarea anterior. Airflow lo lee de XCom por ti
        porque usamos TaskFlow API (@task).
        """
        ranked_songs: list[dict] = sorted(
            all_songs, key=lambda s: s["streams"], reverse=True
        )[:TOP_N]

        log.info("Top %d calculado:", TOP_N)
        for rank, song in enumerate(ranked_songs, start=1):
            log.info(
                "  #%d %s - %s (%s streams)",
                rank, song.get("title", "?"), song.get("artist", "?"),
                f"{song.get('streams', 0):,}",
            )
        return ranked_songs  # ← Esto también se guarda en XCom

    @task
    def show_daily_summary(top_songs: list[dict]) -> None:
        """
        Muestra el resumen final.

        XCOM: Recibe el top 5 de la tarea anterior.

        Esta tarea no retorna nada, así que no guarda XCom.
        """
        total_streams: int = sum(song.get("streams", 0) for song in top_songs)
        number_one: dict = top_songs[0] if top_songs else {}
        average_streams: int = total_streams // max(len(top_songs), 1)

        log.info("=" * 40)
        log.info("RESUMEN SPOTIFY DE HOY")
        log.info("=" * 40)
        log.info("Streams totales del top %d: %s", TOP_N, f"{total_streams:,}")
        log.info(
            "#1 del día: %s de %s",
            number_one.get("title", "N/A"), number_one.get("artist", "N/A"),
        )
        log.info("Promedio de streams: %s", f"{average_streams:,}")
        log.info("=" * 40)

    # --- Dependencias: cada llamada pasa el resultado a la siguiente ---
    # Airflow usa XCom internamente para mover los datos entre tareas.
    all_songs = fetch_songs()
    top_songs = rank_top_songs(all_songs)       # all_songs viene de XCom
    show_daily_summary(top_songs)               # top_songs viene de XCom


example_spotify_xcom()
