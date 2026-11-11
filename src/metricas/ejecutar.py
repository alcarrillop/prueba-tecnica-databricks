"""Corre los archivos SQL de las métricas desde una sesión de Spark.

En el job, `metrica_1.sql`, `metrica_2.sql` y `verificar_metricas.sql` corren como tareas
SQL en el warehouse. Las pruebas (Databricks Connect) y la verificación manual corren
exactamente los mismos archivos con esta función, pasando los mismos parámetros:

    catalogo, esquema   dónde están las tablas de origen y dónde quedan las de destino.
    prefijo             prefijo de todos los nombres de tabla: '' en el job y
                        'test_<sufijo>_' en las pruebas (lee test_<sufijo>_plata_* y
                        escribe test_<sufijo>_oro_*).

Ejemplo (pruebas):

    from metricas.ejecutar import construir, verificar
    construir(spark, "workspace", "prueba_tecnica", prefijo="test_ab12_")
    resumen = verificar(spark, "workspace", "prueba_tecnica", prefijo="test_ab12_")
"""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import Row, SparkSession

DIRECTORIO = Path(__file__).resolve().parent

METRICA_1 = "metrica_1.sql"
METRICA_2 = "metrica_2.sql"
VERIFICAR = "verificar_metricas.sql"

# Nombres de los objetos que crean los archivos (sin catálogo, esquema ni prefijo).
TABLA_METRICA_1 = "oro_contratados_trimestre_2021"
TABLA_METRICA_2 = "oro_departamentos_sobre_promedio_2021"
SUFIJO_VISTA = "_ordenada"

# Una sentencia termina en ';' al final de una línea (formato de los archivos .sql).
_FIN_DE_SENTENCIA = re.compile(r";[ \t]*$", re.MULTILINE)


def sentencias(archivo: str) -> list[str]:
    """Las sentencias de un archivo SQL de esta carpeta, en orden y sin las partes que
    solo tienen comentarios."""
    texto = (DIRECTORIO / archivo).read_text(encoding="utf-8")
    return [parte.strip() for parte in _FIN_DE_SENTENCIA.split(texto) if _tiene_codigo(parte)]


def _tiene_codigo(parte: str) -> bool:
    return any(
        linea.strip() and not linea.strip().startswith("--") for linea in parte.splitlines()
    )


def parametros(catalogo: str, esquema: str, prefijo: str = "") -> dict[str, str]:
    return {"catalogo": catalogo, "esquema": esquema, "prefijo": prefijo}


def ejecutar(
    spark: SparkSession, archivo: str, catalogo: str, esquema: str, prefijo: str = ""
) -> list[Row]:
    """Corre todas las sentencias del archivo, en orden, y devuelve las filas de la última.

    Cada sentencia se ejecuta completa (`collect`), así un chequeo con raise_error falla
    aquí igual que en la tarea SQL del job.
    """
    args = parametros(catalogo, esquema, prefijo)
    filas: list[Row] = []
    for sentencia in sentencias(archivo):
        filas = spark.sql(sentencia, args=args).collect()
    return filas


def construir(spark: SparkSession, catalogo: str, esquema: str, prefijo: str = "") -> None:
    """Reconstruye las dos métricas (tablas y vistas _ordenada)."""
    for archivo in (METRICA_1, METRICA_2):
        ejecutar(spark, archivo, catalogo, esquema, prefijo)


def verificar(spark: SparkSession, catalogo: str, esquema: str, prefijo: str = "") -> Row:
    """Corre los chequeos de verificar_metricas.sql (falla si alguno no da 0) y devuelve
    la fila de resumen."""
    return ejecutar(spark, VERIFICAR, catalogo, esquema, prefijo)[0]
