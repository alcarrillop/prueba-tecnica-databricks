"""Sesión de Spark para las pruebas: Databricks Connect sobre serverless.

El perfil es DEFAULT y se puede cambiar con la variable DATABRICKS_CONFIG_PROFILE.
Las pruebas de reglas corren sobre DataFrames en memoria (no crean tablas). Las marcadas
`tablas` crean tablas y vistas `test_<sufijo>_*` en el esquema del proyecto y las borran
al terminar con `prefijo_temporal`.
"""

import os
import uuid

import pytest
from databricks.connect import DatabricksSession

from fabricas import CATALOGO, ESQUEMA


@pytest.fixture(scope="session")
def spark():
    perfil = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    sesion = DatabricksSession.builder.profile(perfil).serverless(True).getOrCreate()
    # Las reglas no deben depender de la zona de la sesión; se fija UTC como base y las
    # pruebas que lo necesitan la cambian a propósito.
    sesion.conf.set("spark.sql.session.timeZone", "UTC")
    yield sesion


@pytest.fixture
def sesion_bogota(spark):
    """Cambia la zona horaria de la sesión a America/Bogota (UTC-5) durante una prueba."""
    anterior = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "America/Bogota")
    yield
    spark.conf.set("spark.sql.session.timeZone", anterior)


def borrar_con_prefijo(spark, prefijo: str) -> None:
    """Borra las tablas y vistas cuyo nombre empieza por `prefijo` (único por prueba)."""
    assert prefijo.startswith("test_") and len(prefijo) > 10, prefijo
    for fila in spark.sql(f"SHOW TABLES IN `{CATALOGO}`.`{ESQUEMA}` LIKE '{prefijo}*'").collect():
        nombre = f"`{CATALOGO}`.`{ESQUEMA}`.`{fila['tableName']}`"
        try:
            spark.sql(f"DROP TABLE IF EXISTS {nombre}")
        except Exception:  # es una vista
            spark.sql(f"DROP VIEW IF EXISTS {nombre}")


@pytest.fixture
def prefijo_temporal(spark):
    """Fábrica de prefijos `test_<sufijo>_`; al final borra todo lo creado con ellos."""
    creados = []

    def nuevo() -> str:
        prefijo = f"test_{uuid.uuid4().hex[:10]}_"
        creados.append(prefijo)
        return prefijo

    yield nuevo
    for prefijo in creados:
        borrar_con_prefijo(spark, prefijo)
