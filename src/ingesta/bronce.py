"""Ingesta de los CSV del Volume `landing` a las tablas crudas `bronce_*`.

Capa cruda (bronce): una fila por cada línea de cada archivo, con los valores tal como
llegaron. No se recorta, no se cambian mayúsculas, no se tipa, no se descarta ni se
deduplica nada. Las reglas de calidad son de la Sección 2 (src/calidad/).

Mecanismo:
  - Auto Loader (`cloudFiles`) con `trigger(availableNow=True)` y un checkpoint por tabla
    en el Volume `checkpoints`. El checkpoint recuerda qué archivos ya se procesaron:
    cada corrida solo lee archivos nuevos.
  - Cada archivo se lee completo como texto (`wholetext`) para poder numerar sus líneas
    y guardar la línea original. El lector CSV de Spark no da número de línea, se salta
    las líneas vacías y descarta en silencio las columnas de más.
  - Cada línea se parte por comas con `split(linea, ',', -1)`, no con un parser CSV:
      * un campo vacío queda como '' y un campo que no venía en la línea queda NULL
        (`from_csv` convierte ambos en NULL);
      * las comillas no se interpretan: llegan literales (los archivos no usan comillas);
      * las columnas de más no se pierden: siguen en `_linea_original`.
  - Escritura append a Delta. Las tablas se crean con DDL explícito y
    `delta.appendOnly = true`: nadie puede modificar ni borrar filas crudas.

Las funciones de transformación no dependen de Auto Loader y sirven igual para un
DataFrame estático (por ejemplo, en pruebas).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

VOLUMEN_LANDING = "landing"
VOLUMEN_CHECKPOINTS = "checkpoints"

COMENTARIO_ESQUEMA = "Prueba técnica de talento humano: tablas bronce, plata, cuarentena y oro."
COMENTARIO_LANDING = (
    "Archivos CSV tal como llegan del sistema viejo, una carpeta por entidad. "
    "No se sobrescriben."
)
COMENTARIO_CHECKPOINTS = (
    "Checkpoints de las cargas incrementales, uno por tabla. "
    "Borrarlos reinicia la incrementalidad."
)


@dataclass(frozen=True)
class Entidad:
    """Una entidad del enunciado: su carpeta en landing, su tabla bronce y sus columnas."""

    carpeta: str
    tabla: str
    columnas: tuple[tuple[str, str], ...]  # (nombre, descripción del enunciado)

    @property
    def nombres_columnas(self) -> list[str]:
        return [nombre for nombre, _ in self.columnas]


ENTIDADES: tuple[Entidad, ...] = (
    Entidad(
        carpeta="departamentos",
        tabla="bronce_departamentos",
        columnas=(
            ("id", "Id del departamento"),
            ("departamento", "Nombre del departamento"),
        ),
    ),
    Entidad(
        carpeta="cargos",
        tabla="bronce_cargos",
        columnas=(
            ("id", "Id del cargo"),
            ("cargo", "Nombre del cargo"),
        ),
    ),
    Entidad(
        carpeta="empleados",
        tabla="bronce_empleados",
        columnas=(
            ("id", "Id del empleado"),
            ("nombre", "Nombre y apellido"),
            ("fecha_contratacion", "Fecha y hora de contratación, ISO en UTC"),
            ("id_departamento", "Departamento para el que fue contratado"),
            ("id_cargo", "Cargo para el que fue contratado"),
        ),
    ),
)

# Columnas de trazabilidad, iguales en todas las tablas bronce.
COLUMNAS_TRAZABILIDAD: tuple[tuple[str, str, str], ...] = (
    (
        "_archivo_origen",
        "STRING",
        "Ruta del archivo en el Volume del que salió la fila (_metadata.file_path).",
    ),
    (
        "_archivo_modificado_en",
        "TIMESTAMP",
        "Fecha de modificación del archivo en el Volume (_metadata.file_modification_time): "
        "es el momento en que se subió, no cuando el sistema viejo lo exportó.",
    ),
    (
        "_numero_linea",
        "INT",
        "Número de la línea dentro del archivo, empezando en 1.",
    ),
    (
        "_linea_original",
        "STRING",
        "Texto exacto de la línea, sin el terminador de línea (\\r\\n o \\n).",
    ),
    (
        "_ingestado_en",
        "TIMESTAMP",
        "Momento en que la corrida de ingesta escribió la fila.",
    ),
)

_NOMBRE_VALIDO = re.compile(r"^[A-Za-z0-9_]+$")


def _validar_nombre(valor: str, que: str) -> str:
    if not valor or not _NOMBRE_VALIDO.match(valor):
        raise ValueError(f"{que} inválido: {valor!r} (solo letras, números y _).")
    return valor


def nombre_tabla(catalogo: str, esquema: str, entidad: Entidad) -> str:
    return f"`{catalogo}`.`{esquema}`.`{entidad.tabla}`"


def ruta_landing(catalogo: str, esquema: str, entidad: Entidad) -> str:
    return f"/Volumes/{catalogo}/{esquema}/{VOLUMEN_LANDING}/{entidad.carpeta}/"


def ruta_checkpoint(catalogo: str, esquema: str, entidad: Entidad) -> str:
    return f"/Volumes/{catalogo}/{esquema}/{VOLUMEN_CHECKPOINTS}/{entidad.tabla}/"


def _sql_texto(valor: str) -> str:
    return "'" + valor.replace("\\", "\\\\").replace("'", "\\'") + "'"


def ddl_tabla(catalogo: str, esquema: str, entidad: Entidad) -> str:
    """CREATE TABLE IF NOT EXISTS con el esquema explícito y comentarios por columna."""
    columnas = [
        f"`{nombre}` STRING COMMENT "
        + _sql_texto(
            f"{descripcion}. Texto tal como llegó: '' = campo vacío, "
            "NULL = el campo no venía en la línea."
        )
        for nombre, descripcion in entidad.columnas
    ]
    columnas += [
        f"`{nombre}` {tipo} COMMENT {_sql_texto(descripcion)}"
        for nombre, tipo, descripcion in COLUMNAS_TRAZABILIDAD
    ]
    comentario = (
        f"Capa cruda (bronce) de {entidad.carpeta}: una fila por línea de cada archivo de "
        f"landing/{entidad.carpeta}/, valores sin tipar tal como llegaron. Solo admite "
        "agregar filas."
    )
    separador = ",\n  "
    return (
        f"CREATE TABLE IF NOT EXISTS {nombre_tabla(catalogo, esquema, entidad)} (\n  "
        f"{separador.join(columnas)}\n)\n"
        "USING DELTA\n"
        f"COMMENT {_sql_texto(comentario)}\n"
        "TBLPROPERTIES ('delta.appendOnly' = 'true')"
    )


def crear_objetos(spark: SparkSession, catalogo: str, esquema: str) -> None:
    """Crea esquema, Volumes y tablas bronce solo si no existen."""
    _validar_nombre(catalogo, "Catálogo")
    _validar_nombre(esquema, "Esquema")
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS `{catalogo}`.`{esquema}` "
        f"COMMENT {_sql_texto(COMENTARIO_ESQUEMA)}"
    )
    for volumen, comentario in (
        (VOLUMEN_LANDING, COMENTARIO_LANDING),
        (VOLUMEN_CHECKPOINTS, COMENTARIO_CHECKPOINTS),
    ):
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS `{catalogo}`.`{esquema}`.`{volumen}` "
            f"COMMENT {_sql_texto(comentario)}"
        )
    for entidad in ENTIDADES:
        spark.sql(ddl_tabla(catalogo, esquema, entidad))


def partir_en_lineas(archivos: DataFrame, columnas: list[str]) -> DataFrame:
    """Convierte cada archivo (una fila con todo su texto) en una fila por línea.

    Entrada: columnas `value` (texto completo del archivo), `_archivo_origen` y
    `_archivo_modificado_en`. Salida: las columnas de datos (STRING) más las de
    trazabilidad, en el orden de la tabla.

    - Las líneas se separan por '\\n' y a cada una se le quita solo un '\\r' final
      (terminador CRLF). Nada más se modifica.
    - El último elemento vacío que deja el salto de línea final del archivo no es una
      línea y se descarta. Las líneas vacías en medio del archivo sí se conservan.
    """
    lineas = (
        archivos.select(
            "_archivo_origen",
            "_archivo_modificado_en",
            F.size(F.split("value", "\n", -1)).alias("_total_elementos"),
            F.posexplode(F.split("value", "\n", -1)).alias("_posicion", "_linea_con_terminador"),
        )
        .where(
            ~(
                (F.col("_posicion") == F.col("_total_elementos") - 1)
                & (F.col("_linea_con_terminador") == "")
            )
        )
        .withColumn(
            "_linea_original", F.regexp_replace("_linea_con_terminador", "\\r\\z", "")
        )
        .withColumn("_campos", F.split("_linea_original", ",", -1))
    )
    datos: list[Column] = [
        F.expr(f"try_element_at(_campos, {i})").cast("string").alias(nombre)
        for i, nombre in enumerate(columnas, start=1)
    ]
    trazabilidad: list[Column] = [
        F.col("_archivo_origen").cast("string").alias("_archivo_origen"),
        F.col("_archivo_modificado_en").cast("timestamp").alias("_archivo_modificado_en"),
        (F.col("_posicion") + 1).cast("int").alias("_numero_linea"),
        F.col("_linea_original").cast("string").alias("_linea_original"),
        F.current_timestamp().alias("_ingestado_en"),
    ]
    return lineas.select(*datos, *trazabilidad)


def leer_archivos_nuevos(spark: SparkSession, ruta: str) -> DataFrame:
    """Stream de Auto Loader: una fila por archivo nuevo con su texto completo y metadatos."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .option("encoding", "UTF-8")
        .option("pathGlobFilter", "*.csv")
        # Archivos ya existentes en la primera corrida: se procesan.
        .option("cloudFiles.includeExistingFiles", "true")
        # Un archivo reemplazado con el mismo nombre NO se vuelve a procesar.
        .option("cloudFiles.allowOverwrites", "false")
        .load(ruta)
        .select(
            "value",
            F.col("_metadata.file_path").alias("_archivo_origen"),
            F.col("_metadata.file_modification_time").alias("_archivo_modificado_en"),
        )
    )


def cargar_entidad(spark: SparkSession, catalogo: str, esquema: str, entidad: Entidad) -> dict:
    """Procesa los archivos nuevos de una entidad y devuelve conteos antes y después."""
    tabla = nombre_tabla(catalogo, esquema, entidad)
    filas_antes = spark.table(tabla).count()

    consulta = (
        partir_en_lineas(
            leer_archivos_nuevos(spark, ruta_landing(catalogo, esquema, entidad)),
            entidad.nombres_columnas,
        )
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", ruta_checkpoint(catalogo, esquema, entidad))
        .trigger(availableNow=True)
        .queryName(f"ingesta_{entidad.tabla}")
        .toTable(tabla)
    )
    consulta.awaitTermination()

    filas_despues = spark.table(tabla).count()
    return {
        "tabla": f"{catalogo}.{esquema}.{entidad.tabla}",
        "filas_antes": filas_antes,
        "filas_nuevas": filas_despues - filas_antes,
        "filas_despues": filas_despues,
    }


def cargar_todo(spark: SparkSession, catalogo: str, esquema: str) -> list[dict]:
    """Crea lo que falte y carga las tres entidades, una después de otra."""
    crear_objetos(spark, catalogo, esquema)
    return [cargar_entidad(spark, catalogo, esquema, entidad) for entidad in ENTIDADES]
