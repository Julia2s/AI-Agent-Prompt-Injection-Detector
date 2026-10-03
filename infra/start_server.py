"""Build a correctly escaped SQLAlchemy URI without shell interpolation."""

import os

from sqlalchemy.engine import URL

url = URL.create(
    "postgresql+psycopg2",
    username=os.environ["MLFLOW_DB_USER"],
    password=os.environ["MLFLOW_DB_PASSWORD"],
    host="mlflow-db",
    port=5432,
    database=os.environ["MLFLOW_DB_NAME"],
)
os.execvp(
    "mlflow",
    [
        "mlflow",
        "server",
        "--host",
        "0.0.0.0",
        "--port",
        "5000",
        "--backend-store-uri",
        url.render_as_string(hide_password=False),
        "--serve-artifacts",
        "--artifacts-destination",
        "/mlartifacts",
    ],
)
