"""Datos de prueba hechos a mano, con la forma exacta del contrato de las tablas bronce.

Contrato de ingesta: columnas de datos en STRING, más `_archivo_origen`,
`_archivo_modificado_en`, `_numero_linea` (desde 1), `_linea_original` (sin \\r\\n) e
`_ingestado_en`. Aquí no se reimplementa ninguna regla: solo se arman filas.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from calidad.reglas import Entidad

UTC = timezone.utc
INGESTA_1 = datetime(2026, 11, 1, 6, 0, 0, tzinfo=UTC)
INGESTA_2 = datetime(2026, 11, 5, 6, 0, 0, tzinfo=UTC)
LANDING = "/Volumes/workspace/prueba_tecnica/landing"
LOTE_1 = f"{LANDING}/empleados/empleados_contratados_lote1.csv"
LOTE_2 = f"{LANDING}/empleados/empleados_contratados_lote2.csv"

# Una fila válida de empleado (los ids 1 y 2 existen en departamentos y cargos de prueba).
VALIDA = ("1", "Ana Pérez", "2021-07-27T16:02:08Z", "1", "1")


def esquema_bronce(entidad: Entidad) -> str:
    datos = ", ".join(f"`{c}` STRING" for c in entidad.columnas)
    return (
        f"{datos}, _archivo_origen STRING, _archivo_modificado_en TIMESTAMP, "
        "_numero_linea INT, _linea_original STRING, _ingestado_en TIMESTAMP"
    )


def fila(*valores, linea: int | None = None, original: str | None = None) -> dict:
    """Una fila cruda. `original` reemplaza la línea cruda (para probar la estructura)."""
    return {"valores": tuple(valores), "linea": linea, "original": original}


def bronce(
    spark: SparkSession,
    entidad: Entidad,
    filas: list,
    archivo: str = LOTE_1,
    ingestado: datetime = INGESTA_1,
) -> DataFrame:
    """Filas bronce de un archivo. Cada fila es una tupla de valores o `fila(...)`.
    El número de línea es la posición en la lista (desde 1) salvo que se indique."""
    registros = []
    for posicion, f in enumerate(filas, start=1):
        f = f if isinstance(f, dict) else fila(*f)
        valores = f["valores"]
        assert len(valores) == len(entidad.columnas), valores
        original = f["original"]
        if original is None:
            original = ",".join("" if v is None else v for v in valores)
        registros.append(
            (*valores, archivo, ingestado, f["linea"] or posicion, original, ingestado)
        )
    return spark.createDataFrame(registros, esquema_bronce(entidad))


def ids(spark: SparkSession, valores: list[int]) -> DataFrame:
    """Ids existentes de una dimensión (lo que `validar_referencias` lee de plata)."""
    return spark.createDataFrame([(v,) for v in valores], "id INT")


def motivos_por_linea(df: DataFrame) -> dict:
    """{número de línea: [motivos]} de un DataFrame evaluado o de cuarentena."""
    return {r["_numero_linea"]: list(r["motivos"]) for r in df.select("_numero_linea", "motivos").collect()}


def motivos_por_clave(df: DataFrame) -> dict:
    """{(archivo, línea): [motivos]}, para pruebas con más de un archivo."""
    return {
        (r["_archivo_origen"], r["_numero_linea"]): list(r["motivos"])
        for r in df.select("_archivo_origen", "_numero_linea", "motivos").collect()
    }


def claves(df: DataFrame) -> set:
    return {(r["_archivo_origen"], r["_numero_linea"]) for r in df.select("_archivo_origen", "_numero_linea").collect()}


def micros_utc(texto_iso_z: str) -> int:
    """Microsegundos desde 1970 (UTC) de una fecha ISO con Z: el valor esperado."""
    return int(datetime.strptime(texto_iso_z, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).timestamp()) * 1_000_000
