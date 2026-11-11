"""Sesión de Spark para las pruebas: Databricks Connect sobre serverless.

El perfil es DEFAULT y se puede cambiar con la variable DATABRICKS_CONFIG_PROFILE.
Las pruebas de reglas corren sobre DataFrames en memoria (no crean tablas).
"""

import os

import pytest
from databricks.connect import DatabricksSession


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
