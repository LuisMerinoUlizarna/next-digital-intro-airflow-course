"""
## Estados de las tareas en Airflow - Ejemplo educativo

Este DAG muestra los estados más comunes que puede tener una tarea en Airflow.
Al ejecutarlo verás tareas en distintos colores en la UI, cada una representando
un estado diferente.

### Estados que verás:

| Color    | Estado       | Significado                                      |
|----------|--------------|--------------------------------------------------|
| 🟢 Verde  | **success**  | La tarea terminó correctamente.                  |
| 🔴 Rojo   | **failed**   | La tarea lanzó una excepción.                    |
| 🟡 Amarillo | **up_for_retry** | Falló pero se va a reintentar.             |
| 🟠 Naranja | **upstream_failed** | No se ejecutó porque una anterior falló.  |
| 🩷 Rosa   | **skipped**  | Se saltó (por branching o condición).            |
| 🟣 Morado | **running**  | Se está ejecutando ahora mismo.                  |

### Flujo del pipeline:

```
  always_succeeds ──→ depends_on_success ──→ final_report
        |                                        ↑
        ├──→ check_condition                     |
        |       /    \\                           |
        |  skipped   executed                    |
        |                                        |
  always_fails ──→ retries_then_fails ──→ upstream_failed_task
```

Ejecuta el DAG y observa los colores en la vista Grid o Graph.

### Documentación oficial de Airflow

- [Task lifecycle](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#task-instances)
- [Trigger rules](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules)
- [Retries](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#retries)
- [Branching](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#branchpythonoperator)
"""

import logging
import random

from airflow.operators.python import BranchPythonOperator
from airflow.sdk import dag, task
from pendulum import datetime, duration

log = logging.getLogger(__name__)

# Contador para simular reintentos (en un entorno real esto vendría de un servicio externo)
RETRY_MAX_ATTEMPTS = 3


@dag(
    start_date=datetime(2025, 4, 1),
    schedule=None,  # Solo ejecución manual (trigger)
    doc_md=__doc__,
    default_args={
        "owner": "Universidad",
        "retries": 0,  # Sin reintentos por defecto
        "retry_delay": duration(seconds=5),
    },
    tags=["example", "educativo", "estados"],
    is_paused_upon_creation=True,  # El DAG se crea pausado; hay que activarlo manualmente en la UI
    catchup=False,  # No ejecuta runs pasados; solo programa desde ahora en adelante
)
def example_task_states():
    """DAG que demuestra los diferentes estados de las tareas en Airflow."""

    # =========================================================
    # ESTADO: SUCCESS (verde)
    # Una tarea que siempre termina correctamente.
    # =========================================================

    @task
    def always_succeeds() -> str:
        """Esta tarea siempre tiene éxito → estado SUCCESS (verde)."""
        message = "Esta tarea completó sin errores."
        log.info(message)
        return message

    # =========================================================
    # ESTADO: FAILED (rojo)
    # Una tarea que lanza una excepción intencionalmente.
    # retries=0 para que falle directamente sin reintentar.
    # =========================================================

    @task(retries=0)
    def always_fails() -> None:
        """Esta tarea siempre falla → estado FAILED (rojo)."""
        log.info("A punto de fallar intencionalmente...")
        raise ValueError(
            "Error simulado para demostrar el estado FAILED. "
            "En un caso real, esto podría ser un timeout de API, "
            "un archivo que no existe, o datos corruptos."
        )

    # =========================================================
    # ESTADO: UP_FOR_RETRY (amarillo)
    # Una tarea que falla pero tiene reintentos configurados.
    # Verás el estado amarillo mientras espera reintentar.
    # Después de agotar los reintentos, pasará a FAILED (rojo).
    # =========================================================

    @task(retries=RETRY_MAX_ATTEMPTS, retry_delay=duration(seconds=10))
    def retries_then_fails() -> None:
        """
        Esta tarea falla y reintenta 3 veces → estado UP_FOR_RETRY (amarillo).
        Tras agotar los reintentos → estado FAILED (rojo).
        """
        log.info("Intentando conectar a un servicio ficticio...")
        raise ConnectionError(
            "No se pudo conectar al servicio. "
            "Airflow reintentará esta tarea automáticamente."
        )

    # =========================================================
    # ESTADO: UPSTREAM_FAILED (naranja)
    # Esta tarea depende de una que falla, así que nunca se ejecuta.
    # Airflow la marca como upstream_failed automáticamente.
    # =========================================================

    @task
    def upstream_failed_task() -> None:
        """
        Esta tarea nunca se ejecuta → estado UPSTREAM_FAILED (naranja).
        Depende de 'retries_then_fails' que siempre falla.
        """
        log.info("Esto nunca se imprime porque la tarea anterior falló.")

    # =========================================================
    # ESTADO: SKIPPED (rosa)
    # Usando branching, una de las dos tareas se salta.
    # La tarea no elegida queda en estado SKIPPED.
    # =========================================================

    def _choose_path() -> str:
        """Siempre elige path_executed, dejando path_skipped en estado SKIPPED (rosa)."""
        log.info("Camino elegido: path_executed (path_skipped quedará SKIPPED)")
        return "path_executed"

    branch_operator = BranchPythonOperator(
        task_id="check_condition",
        python_callable=_choose_path,
    )

    @task
    def path_executed() -> None:
        """Si el branch la elige → SUCCESS. Si no → SKIPPED (rosa)."""
        log.info("Este camino fue elegido por el branch.")

    @task
    def path_skipped() -> None:
        """Si el branch la elige → SUCCESS. Si no → SKIPPED (rosa)."""
        log.info("Este camino fue elegido por el branch.")

    # =========================================================
    # ESTADO: SUCCESS con dependencia
    # Esta tarea solo depende de 'always_succeeds', así que
    # se ejecuta correctamente aunque otras tareas fallen.
    # =========================================================

    @task
    def depends_on_success(upstream_message: str) -> str:
        """
        Depende solo de 'always_succeeds' → estado SUCCESS (verde).
        Las tareas que fallan en otras ramas no le afectan.
        """
        log.info("Tarea upstream dijo: %s", upstream_message)
        return "Cadena de éxito completada."

    # =========================================================
    # TAREA FINAL con trigger_rule
    # Usa trigger_rule="all_done" para ejecutarse sin importar
    # si las tareas anteriores fallaron o se saltaron.
    # =========================================================

    @task(trigger_rule="all_done")
    def final_report(**context) -> None:
        """
        Se ejecuta siempre (trigger_rule=all_done) → estado SUCCESS (verde).
        Recopila el estado de todas las tareas anteriores.
        """
        log.info("=" * 50)
        log.info("REPORTE FINAL DE ESTADOS")
        log.info("=" * 50)
        log.info("always_succeeds    → SUCCESS (verde)")
        log.info("always_fails       → FAILED (rojo)")
        log.info("retries_then_fails → UP_FOR_RETRY (amarillo) → FAILED (rojo)")
        log.info("upstream_failed    → UPSTREAM_FAILED (naranja)")
        log.info("check_condition    → SUCCESS (verde)")
        log.info("path_executed/skipped → uno SUCCESS, otro SKIPPED (rosa)")
        log.info("depends_on_success → SUCCESS (verde)")
        log.info("final_report       → SUCCESS (verde, trigger_rule=all_done)")
        log.info("=" * 50)

    # =========================================================
    # Dependencias
    # =========================================================

    # Rama exitosa
    success_msg = always_succeeds()
    chain_success = depends_on_success(success_msg)

    # Rama que falla → upstream_failed
    fail = always_fails()
    retry_fail = retries_then_fails()
    upstream_skip = upstream_failed_task()
    retry_fail >> upstream_skip

    # Rama con branching → skipped
    # El branch depende de always_succeeds para que se ejecute correctamente.
    # Así el branch elige un camino (SUCCESS) y descarta el otro (SKIPPED).
    branch_operator.set_upstream(success_msg)
    executed = path_executed()
    skipped = path_skipped()
    branch_operator >> [executed, skipped]

    # Reporte final espera a todo
    [chain_success, upstream_skip, executed, skipped] >> final_report()


example_task_states()
