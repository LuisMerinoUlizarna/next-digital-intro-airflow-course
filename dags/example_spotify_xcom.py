"""
## Spotify Top 5 - Ejemplo de XCom con API real

Este DAG enseña cómo funciona XCom en Airflow usando la API real de Spotify
para obtener las canciones más populares del momento.

### ¿Qué es XCom?

XCom (cross-communication) es como una "memoria compartida" entre tareas.
Cuando una tarea retorna un valor, Airflow lo guarda automáticamente.
La siguiente tarea puede leerlo y usarlo.

### Flujo:

```
fetch_top_songs  →  rank_top_songs  →  show_daily_summary
```

- **fetch_top_songs**: Obtiene canciones reales de la playlist "Today's Top Hits"
  de Spotify. El `return` guarda los datos en XCom automáticamente.
- **rank_top_songs**: Recibe esos datos (Airflow los inyecta como parámetro),
  ordena las canciones por popularidad y devuelve el top 5.
- **show_daily_summary**: Recibe el top 5 y muestra el resumen.

### Requisitos previos

1. Crea una app en https://developer.spotify.com/dashboard
2. Copia tu **Client ID** y **Client Secret**.
3. Reemplaza las constantes `SPOTIFY_CLIENT_ID` y `SPOTIFY_CLIENT_SECRET` en este archivo.

La autenticación usa Client Credentials Flow (no requiere login de usuario).

### Documentación oficial de Airflow

- [XCom](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Conceptos: Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)
"""

import logging

import requests
from airflow.sdk import dag, task
from pendulum import datetime, duration

# --------------- #
# DAG Constants   #
# --------------- #

# ⚠️ Reemplaza con tus credenciales de https://developer.spotify.com/dashboard
SPOTIFY_CLIENT_ID = "TU_CLIENT_ID"
SPOTIFY_CLIENT_SECRET = "TU_CLIENT_SECRET"

SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"

# Playlist "Today's Top Hits" de Spotify (pública)
TOP_HITS_PLAYLIST_ID = "37i9dQZF1DXcBWIGoYBM5M"

TOP_N = 5
HTTP_TIMEOUT_SECONDS = 15
MAX_TRACKS_TO_FETCH = 20

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
    def fetch_top_songs() -> list[dict]:
        """
        Obtiene canciones reales de la playlist "Today's Top Hits" de Spotify.

        Usa Client Credentials Flow: solo necesita Client ID y Secret,
        no requiere login de usuario.

        XCOM: Al hacer ``return``, Airflow guarda este valor
        automáticamente para que otras tareas lo lean.
        Puedes verlo en la UI: click en la tarea → XCom.
        """
        # Paso 1: Obtener token de acceso
        token_response = requests.post(
            SPOTIFY_TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        token_response.raise_for_status()
        access_token: str = token_response.json().get("access_token", "")

        # Paso 2: Obtener canciones de la playlist
        playlist_url = (
            f"{SPOTIFY_API_BASE_URL}/playlists/{TOP_HITS_PLAYLIST_ID}/tracks"
            f"?limit={MAX_TRACKS_TO_FETCH}&fields=items(track(name,artists(name),popularity,album(name)))"
        )
        headers = {"Authorization": f"Bearer {access_token}"}
        playlist_response = requests.get(playlist_url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
        playlist_response.raise_for_status()

        # Paso 3: Extraer datos relevantes de cada canción
        items: list[dict] = playlist_response.json().get("items", [])
        songs: list[dict] = []
        for item in items:
            track: dict = item.get("track", {})
            if not track:
                continue
            artists_list: list[dict] = track.get("artists", [])
            artist_name: str = artists_list[0].get("name", "Desconocido") if artists_list else "Desconocido"
            songs.append({
                "title": track.get("name", "Sin título"),
                "artist": artist_name,
                "album": track.get("album", {}).get("name", "Sin álbum"),
                "popularity": track.get("popularity", 0),
            })

        log.info("Obtenidas %d canciones de 'Today's Top Hits'.", len(songs))
        return songs  # ← Esto se guarda en XCom

    @task
    def rank_top_songs(all_songs: list[dict]) -> list[dict]:
        """
        Ordena las canciones por popularidad y devuelve el top 5.

        XCOM: El parámetro ``all_songs`` viene automáticamente de
        la tarea anterior. Airflow lo lee de XCom por ti
        porque usamos TaskFlow API (@task).
        """
        ranked_songs: list[dict] = sorted(
            all_songs, key=lambda s: s.get("popularity", 0), reverse=True
        )[:TOP_N]

        log.info("Top %d por popularidad:", TOP_N)
        for rank, song in enumerate(ranked_songs, start=1):
            log.info(
                "  #%d %s - %s (popularidad: %d)",
                rank, song.get("title", "?"), song.get("artist", "?"),
                song.get("popularity", 0),
            )
        return ranked_songs  # ← Esto también se guarda en XCom

    @task
    def show_daily_summary(top_songs: list[dict]) -> None:
        """
        Muestra el resumen final del top 5.

        XCOM: Recibe el top 5 de la tarea anterior.

        Esta tarea no retorna nada, así que no guarda XCom.
        """
        if not top_songs:
            log.warning("No hay canciones para mostrar.")
            return

        total_popularity: int = sum(song.get("popularity", 0) for song in top_songs)
        number_one: dict = top_songs[0]
        average_popularity: int = total_popularity // len(top_songs)

        log.info("=" * 50)
        log.info("SPOTIFY TOP %d DE HOY", TOP_N)
        log.info("=" * 50)
        for rank, song in enumerate(top_songs, start=1):
            log.info(
                "  #%d %s - %s [%s] (popularidad: %d)",
                rank,
                song.get("title", "?"),
                song.get("artist", "?"),
                song.get("album", "?"),
                song.get("popularity", 0),
            )
        log.info("-" * 50)
        log.info(
            "#1 del día: %s de %s",
            number_one.get("title", "N/A"), number_one.get("artist", "N/A"),
        )
        log.info("Popularidad promedio del top %d: %d", TOP_N, average_popularity)
        log.info("=" * 50)

    # --- Dependencias: cada llamada pasa el resultado a la siguiente ---
    # Airflow usa XCom internamente para mover los datos entre tareas.
    all_songs = fetch_top_songs()
    top_songs = rank_top_songs(all_songs)       # all_songs viene de XCom
    show_daily_summary(top_songs)               # top_songs viene de XCom


example_spotify_xcom()
