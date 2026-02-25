"""
## ETL básico del clima - Ejemplo educativo

Este DAG demuestra un pipeline ETL (Extract, Transform, Load) sencillo:

1. **Extract**: Obtiene datos del clima de Madrid (actual + últimas 24h) desde Open-Meteo.
2. **Transform**: Procesa los datos y calcula estadísticas básicas.
3. **Chart**: Genera un gráfico de temperatura con matplotlib y lo guarda en disco.

### Cómo ver los resultados

- **Logs**: click en cualquier tarea → pestaña Log.
- **Gráfico PNG**: se guarda en `/tmp/airflow_weather/temperature_chart.png`

No requiere credenciales ni configuración externa.

API utilizada: https://open-meteo.com/ (gratuita, sin API key)

### Documentación oficial de Airflow

- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Conceptos: DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
- [Conceptos: Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)
- [XCom](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
"""

import json
import logging
from pathlib import Path

import requests
from airflow.sdk import dag, task
from pendulum import datetime, duration

# --------------- #
# DAG Constants   #
# --------------- #

OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
MADRID_LATITUDE = 40.42
MADRID_LONGITUDE = -3.70
CURRENT_WEATHER_FIELDS = "temperature_2m,wind_speed_10m,relative_humidity_2m"
HOURLY_WEATHER_FIELDS = "temperature_2m"
TIMEZONE = "Europe/Madrid"
HTTP_TIMEOUT_SECONDS = 30
OUTPUT_DIR = Path("/tmp/airflow_weather")
CHART_FILENAME = "temperature_chart.png"

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
        "retry_delay": duration(seconds=10),
    },
    tags=["example", "etl", "educativo"],
    is_paused_upon_creation=True,  # El DAG se crea pausado; hay que activarlo manualmente en la UI
    catchup=False,  # No ejecuta runs pasados; solo programa desde ahora en adelante
)
def example_weather_etl():
    """Pipeline ETL que obtiene, transforma y grafica datos del clima."""

    @task
    def extract_weather_data() -> dict:
        """Obtiene datos del clima actual y horario de Madrid desde Open-Meteo."""
        request_url = (
            f"{OPEN_METEO_BASE_URL}"
            f"?latitude={MADRID_LATITUDE}&longitude={MADRID_LONGITUDE}"
            f"&current={CURRENT_WEATHER_FIELDS}"
            f"&hourly={HOURLY_WEATHER_FIELDS}"
            f"&timezone={TIMEZONE}"
            f"&forecast_days=1&past_days=1"
        )
        response = requests.get(request_url, timeout=HTTP_TIMEOUT_SECONDS)
        response.raise_for_status()

        raw_data: dict = response.json()
        log.info("Datos del clima obtenidos correctamente desde Open-Meteo.")
        return raw_data

    @task
    def transform_weather_data(raw_weather: dict) -> dict:
        """Extrae y estructura los campos relevantes del JSON crudo."""
        current_conditions: dict = raw_weather.get("current", {})

        temperature_celsius: float = current_conditions.get("temperature_2m", 0.0)
        wind_speed_kmh: float = current_conditions.get("wind_speed_10m", 0.0)
        humidity_percent: float = current_conditions.get("relative_humidity_2m", 0.0)
        observation_time: str = current_conditions.get("time", "unknown")

        hourly_data: dict = raw_weather.get("hourly", {})
        hourly_times: list[str] = hourly_data.get("time", [])
        hourly_temps: list[float] = hourly_data.get("temperature_2m", [])

        weather_report = {
            "city": "Madrid",
            "temperature_celsius": temperature_celsius,
            "wind_speed_kmh": wind_speed_kmh,
            "humidity_percent": humidity_percent,
            "observation_time": observation_time,
            "hourly_times": hourly_times,
            "hourly_temperatures": hourly_temps,
            "summary": (
                f"Madrid: {temperature_celsius}°C, "
                f"viento {wind_speed_kmh} km/h, "
                f"humedad {humidity_percent}%"
            ),
        }

        log.info("Datos transformados: %s", weather_report.get("summary", ""))
        return weather_report

    @task
    def generate_temperature_chart(weather_report: dict) -> None:
        """
        Genera un gráfico de temperatura horaria con matplotlib
        y lo guarda como PNG en /tmp/airflow_weather/.
        """
        from datetime import datetime as dt

        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt

        hourly_times: list[str] = weather_report.get("hourly_times", [])
        hourly_temps: list[float] = weather_report.get("hourly_temperatures", [])

        if not hourly_times or not hourly_temps:
            log.warning("No hay datos horarios para generar el gráfico.")
            return

        time_objects = [dt.fromisoformat(t) for t in hourly_times]

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(
            time_objects, hourly_temps,
            color="#FF6B35", linewidth=2.5, marker="o", markersize=3,
        )
        ax.fill_between(time_objects, hourly_temps, alpha=0.15, color="#FF6B35")

        current_temp = weather_report.get("temperature_celsius", 0.0)
        current_time_str = weather_report.get("observation_time", "")
        if current_time_str and current_time_str != "unknown":
            current_time = dt.fromisoformat(current_time_str)
            ax.axvline(x=current_time, color="#E63946", linestyle="--", alpha=0.6)
            ax.annotate(
                f"  Ahora: {current_temp}°C",
                xy=(current_time, current_temp),
                fontsize=11, fontweight="bold", color="#E63946",
            )

        ax.set_title(
            "Temperatura en Madrid — Últimas 24h y pronóstico",
            fontsize=14, fontweight="bold", pad=12,
        )
        ax.set_xlabel("Hora", fontsize=11)
        ax.set_ylabel("Temperatura (°C)", fontsize=11)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=3))
        fig.autofmt_xdate(rotation=45)
        ax.grid(True, alpha=0.3)
        ax.set_facecolor("#FAFAFA")
        fig.patch.set_facecolor("#FFFFFF")

        wind = weather_report.get("wind_speed_kmh", 0.0)
        humidity = weather_report.get("humidity_percent", 0.0)
        temp_min = min(hourly_temps)
        temp_max = max(hourly_temps)
        stats_text = (
            f"Actual: {current_temp}°C  |  "
            f"Mín: {temp_min}°C  |  Máx: {temp_max}°C  |  "
            f"Viento: {wind} km/h  |  Humedad: {humidity}%"
        )
        fig.text(0.5, 0.01, stats_text, ha="center", fontsize=10, color="#555555")
        plt.tight_layout(rect=[0, 0.04, 1, 1])

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        chart_path = OUTPUT_DIR / CHART_FILENAME
        fig.savefig(chart_path, dpi=120, bbox_inches="tight")
        plt.close(fig)

        log.info("Gráfico guardado en %s", chart_path)
        log.info("Resumen: %s", weather_report.get("summary", "N/A"))

    # --- Dependencias: extract >> transform >> chart ---
    raw_data = extract_weather_data()
    transformed_data = transform_weather_data(raw_data)
    generate_temperature_chart(transformed_data)


example_weather_etl()
