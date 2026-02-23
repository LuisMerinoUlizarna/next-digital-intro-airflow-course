"""
## Pipeline con Amazon S3 - Ejemplo educativo

Este DAG demuestra cómo interactuar con Amazon S3 desde Airflow:

1. **extract_sales_data**: Genera datos de ejemplo (ventas ficticias).
2. **upload_csv_to_s3**: Sube un archivo CSV a un bucket de S3.
3. **read_csv_from_s3**: Lee el archivo desde S3 y muestra su contenido.

### Requisitos previos

1. Tener una cuenta de AWS con acceso a S3 (el free tier incluye 5 GB).
2. Crear un bucket en S3 (por ejemplo: `mi-bucket-universidad`).
3. Configurar una conexión en Airflow:
   - Ve a **Admin > Connections** en la UI de Airflow.
   - Crea una conexión con:
     - **Connection Id**: `aws_default`
     - **Connection Type**: `Amazon Web Services`
     - **AWS Access Key ID**: tu access key
     - **AWS Secret Access Key**: tu secret key
     - **Extra**: `{"region_name": "us-east-1"}` (ajusta tu región)

4. Instalar el provider de Amazon añadiendo a `requirements.txt`:
   ```
   apache-airflow-providers-amazon
   ```

Documentación: https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/

### Documentación oficial de Airflow

- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Gestión de conexiones](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [Amazon provider](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/)
- [S3Hook](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html)
"""

import csv
import io
import logging
from datetime import date

from airflow.sdk import dag, task
from pendulum import datetime, duration

# --------------- #
# DAG Constants   #
# --------------- #

# ⚠️ Cambia esto por el nombre de tu bucket real en AWS
S3_BUCKET_NAME = "mi-bucket-universidad"
S3_KEY_PREFIX = "airflow-demo"
AWS_CONN_ID = "aws_default"
CSV_FIELDNAMES = ["producto", "cantidad", "precio"]

SAMPLE_SALES_DATA: list[dict] = [
    {"producto": "Laptop", "cantidad": 5, "precio": 899.99},
    {"producto": "Mouse", "cantidad": 25, "precio": 19.99},
    {"producto": "Teclado", "cantidad": 15, "precio": 49.99},
    {"producto": "Monitor", "cantidad": 8, "precio": 299.99},
    {"producto": "Auriculares", "cantidad": 30, "precio": 39.99},
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
        "retry_delay": duration(seconds=10),
    },
    tags=["example", "s3", "aws", "educativo"],
    is_paused_upon_creation=True,  # El DAG se crea pausado; hay que activarlo manualmente en la UI
    catchup=False,  # No ejecuta runs pasados; solo programa desde ahora en adelante
)
def example_s3_pipeline():
    """Pipeline que genera datos de ventas, los sube a S3 y los lee de vuelta."""

    @task
    def extract_sales_data() -> str:
        """Genera datos ficticios de ventas y los serializa como CSV."""
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(SAMPLE_SALES_DATA)

        csv_content: str = csv_buffer.getvalue()
        log.info("CSV generado con %d filas.", len(SAMPLE_SALES_DATA))
        return csv_content

    @task
    def upload_csv_to_s3(csv_content: str) -> str:
        """Sube el contenido CSV al bucket de S3 usando S3Hook."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        today_iso: str = date.today().isoformat()
        s3_object_key = f"{S3_KEY_PREFIX}/ventas_{today_iso}.csv"

        s3_hook.load_string(
            string_data=csv_content,
            key=s3_object_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )

        log.info("Archivo subido a s3://%s/%s", S3_BUCKET_NAME, s3_object_key)
        return s3_object_key

    @task
    def read_csv_from_s3(s3_object_key: str) -> None:
        """Lee el CSV desde S3 y calcula estadísticas básicas de ventas."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        raw_csv: str = s3_hook.read_key(
            key=s3_object_key, bucket_name=S3_BUCKET_NAME
        )

        reader = csv.DictReader(io.StringIO(raw_csv))
        sales_rows: list[dict] = list(reader)

        total_revenue: float = sum(
            int(row.get("cantidad", 0)) * float(row.get("precio", 0.0))
            for row in sales_rows
        )

        log.info("Productos leídos desde S3: %d", len(sales_rows))
        log.info("Valor total de ventas: $%,.2f", total_revenue)

        for row in sales_rows:
            quantity = int(row.get("cantidad", 0))
            unit_price = float(row.get("precio", 0.0))
            subtotal = quantity * unit_price
            log.info(
                "  - %s: %d uds x $%.2f = $%,.2f",
                row.get("producto", "Desconocido"), quantity, unit_price, subtotal,
            )

    # --- Dependencias: extract >> upload >> read ---
    csv_data = extract_sales_data()
    s3_key = upload_csv_to_s3(csv_data)
    read_csv_from_s3(s3_key)


example_s3_pipeline()
