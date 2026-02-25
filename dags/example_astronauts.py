"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

# ============================================================================
# TASKFLOW API: en lugar de usar PythonOperator y DAG(), usamos los
# decoradores @dag y @task que simplifican la definición de DAGs y tareas.
# Ver: https://www.astronomer.io/docs/learn/airflow-decorators
# ============================================================================

import requests
from airflow.sdk import Asset, dag, task
from pendulum import datetime, duration


# -------------- #
# DAG Definition #
# -------------- #

# TASKFLOW API - @dag:
# En vez de crear el DAG con "with DAG(...) as dag:" (forma clásica),
# decoramos una función Python con @dag. Todo lo que se defina dentro
# de esta función formará parte del DAG automáticamente.
@dag(
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(seconds=5),
    },
    tags=["example", "space"],
    is_paused_upon_creation=False,
)
def example_astronauts():

    # ---------------- #
    # Task Definitions #
    # ---------------- #

    # TASKFLOW API - @task:
    # En vez de usar PythonOperator(task_id="...", python_callable=func),
    # simplemente decoramos la función con @task y se convierte en una
    # tarea de Airflow. El task_id se toma del nombre de la función.
    @task(
        outlets=[Asset("current_astronauts")]
    )
    def get_astronauts(**context) -> list[dict]:
        """
        Consulta la API de Open Notify para obtener la lista de astronautas
        actualmente en el espacio.

        TASKFLOW API - return automático a XCom:
        Al hacer "return list_of_people_in_space", el valor se guarda
        automáticamente en XCom sin necesidad de llamar a xcom_push().
        Las tareas downstream pueden recibir este valor como argumento.
        """
        try:
            r = requests.get("http://api.open-notify.org/astros.json")
            r.raise_for_status()
            response_data = r.json()
            number_of_people_in_space = response_data.get("number", 0)
            list_of_people_in_space = response_data.get("people", [])
        except:
            print("API currently not available, using hardcoded data instead.")
            number_of_people_in_space = 12
            list_of_people_in_space = [
                {"craft": "ISS", "name": "Marco Alain Sieber"},
                {"craft": "ISS", "name": "Claude Nicollier"},
            ]

        # Este xcom_push es MANUAL y adicional (para guardar un dato extra).
        # El return de abajo ya hace xcom_push automático gracias a TaskFlow.
        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )

        # TASKFLOW API: este return guarda la lista en XCom automáticamente
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        Imprime el nombre del astronauta y la nave en la que viaja.

        TASKFLOW API - dependencias implícitas:
        Esta tarea recibe "person_in_space" como argumento. Cuando se le
        pasa el resultado de get_astronauts(), Airflow infiere la dependencia
        automáticamente (sin necesidad de usar >> o set_downstream).
        """
        craft = person_in_space.get("craft", "Unknown")
        name = person_in_space.get("name", "Unknown")
        print(f"{name} is in space flying on the {craft}! {greeting}")

    # ------------------------------------ #
    # Calling tasks + Setting dependencies #
    # ------------------------------------ #

    # DYNAMIC TASK MAPPING:
    # En vez de crear un número fijo de tareas, usamos .partial() y .expand()
    # para que Airflow cree una instancia de la tarea POR CADA elemento
    # de la lista que devuelve get_astronauts().
    #
    # - .partial(greeting="Hello! :)") → fija el argumento "greeting" con un
    #   valor constante. Todas las copias de la tarea recibirán este mismo valor.
    #
    # - .expand(person_in_space=get_astronauts()) → crea N copias de la tarea,
    #   una por cada astronauta en la lista. Si la API devuelve 12 astronautas,
    #   se crean 12 tareas en paralelo. Si mañana hay 7, se crean 7.
    #   El número se resuelve en TIEMPO DE EJECUCIÓN, no al definir el DAG.
    #
    # TASKFLOW API - dependencia implícita:
    # Al pasar get_astronauts() como argumento, Airflow sabe que
    # print_astronaut_craft depende de get_astronauts sin usar >>.
    
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=get_astronauts()
    )


# Instanciar el DAG llamando a la función decorada con @dag
example_astronauts()
