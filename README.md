# Curso introductorio de Apache Airflow

> Este repositorio está basado en la plantilla oficial de [Astronomer](https://github.com/astronomer/astro-cli), generada con `astro dev init`. Se ha ampliado con DAGs educativos para un curso universitario de introducción a Airflow.

## DAGs de ejemplo incluidos

| DAG | Archivo | Qué enseña |
|-----|---------|------------|
| **Astronauts ETL** | [`example_astronauts.py`](dags/example_astronauts.py) | DAG original de Astronomer. TaskFlow API, dynamic task mapping y llamadas a APIs. |
| **Weather ETL** | [`example_weather_etl.py`](dags/example_weather_etl.py) | Pipeline ETL completo (extract → transform → chart → load). Genera un gráfico de temperatura con matplotlib y lo muestra en la UI de Airflow. API gratuita Open-Meteo, sin credenciales. |
| **Spotify XCom** | [`example_spotify_xcom.py`](dags/example_spotify_xcom.py) | Concepto de XCom explicado paso a paso. Pipeline lineal: obtener canciones → rankear top 5 → mostrar resumen. |
| **Spotify Trends** | [`example_spotify_trends.py`](dags/example_spotify_trends.py) | Branching, tareas en paralelo y trigger rules. Simula un análisis de tendencias musicales con caminos condicionales (pop vs rock). |
| **S3 Pipeline** | [`example_s3_pipeline.py`](dags/example_s3_pipeline.py) | Integración con Amazon S3 (free tier). Genera un CSV de ventas, lo sube a S3 y lo lee de vuelta. Requiere configurar conexión AWS. |
| **Task States** | [`example_task_states.py`](dags/example_task_states.py) | Muestra los estados más comunes de las tareas: success, failed, up_for_retry, upstream_failed y skipped. Ideal para entender los colores de la UI. |
| **Ejercicio: Movie Pipeline** | [`exercise_movie_pipeline.py`](dags/exercise_movie_pipeline.py) | Esqueleto incompleto con TODOs para que los alumnos practiquen. Usa la API de OMDb (películas). |

---

# Contenido del proyecto (Astronomer)

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
  - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://docs.astronomer.io/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

# Deploy Your Project Locally

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either stop your existing Docker containers or change the port.

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

# Deploy Your Project to Astronomer

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://docs.astronomer.io/cloud/deploy-code/

# Contact

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
