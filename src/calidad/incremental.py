"""Procesamiento incremental y repetible de bronce a plata y cuarentena.

Unidad de trabajo: la fila cruda, identificada por (`_archivo_origen`, `_numero_linea`).

1. Pendientes = filas bronce que no están ni en plata ni en cuarentena. Solo esas se
   deciden; repetir una corrida sin filas nuevas no escribe nada.
2. Contexto = filas ya procesadas que comparten id con alguna pendiente. Se reevalúan
   junto con las pendientes para revisar la unicidad contra TODA la historia (D8), sin
   releer el resto de la tabla.
3. Resultado (invariante de D8): el estado final es el mismo que si todos los lotes
   hubieran llegado juntos. Por eso una fila plata puede retirarse a cuarentena cuando
   un lote posterior trae otra versión de su id, y una fila en cuarentena puede ganar un
   motivo (`motivos` se actualiza; `rechazado_en` no cambia).

`planificar` es puro (DataFrames de entrada y de salida) y se prueba en memoria.
`aplicar_plan` y `procesar` escriben en Delta.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from calidad.reglas import (
    CARGOS,
    CLAVE,
    DEPARTAMENTOS,
    EMPLEADOS,
    ESQUEMA_CUARENTENA,
    Entidad,
    a_cuarentena,
    a_plata,
    esquema_plata,
    evaluar,
    id_normalizado,
    nombres_repetidos,
)


# --- Nombres de tablas --------------------------------------------------------------------
@dataclass(frozen=True)
class Tablas:
    """Nombres completos de las tablas. `prefijo` lo usan las pruebas (`test_<sufijo>_`)."""

    catalogo: str
    esquema: str
    prefijo: str = ""

    def _nombre(self, tabla: str) -> str:
        return f"`{self.catalogo}`.`{self.esquema}`.`{self.prefijo}{tabla}`"

    def bronce(self, entidad: Entidad) -> str:
        return self._nombre(f"bronce_{entidad.nombre}")

    def plata(self, entidad: Entidad) -> str:
        return self._nombre(f"plata_{entidad.nombre}")

    @property
    def cuarentena(self) -> str:
        return self._nombre("cuarentena")


# --- Planificación (pura) -----------------------------------------------------------------
@dataclass
class Plan:
    entidad: Entidad
    plata_nuevas: DataFrame  # filas pendientes válidas -> INSERT en plata
    cuarentena: DataFrame  # nuevas en cuarentena, retiradas de plata o con motivos nuevos
    retirar: DataFrame  # claves que salen de plata (conflicto con un lote posterior)


def planificar(
    entidad: Entidad,
    bronce: DataFrame,
    plata: DataFrame,
    cuarentena: DataFrame,
    ids_departamentos: DataFrame | None = None,
    ids_cargos: DataFrame | None = None,
) -> Plan:
    """Decide qué hacer con las filas nuevas de `bronce` dado el estado actual.

    Los DataFrames de entrada deben ser fotos fijas (versiones Delta fijas o datos en
    memoria): las tres salidas se calculan sobre la misma foto aunque se escriban en
    pasos distintos.
    """
    en_plata = plata.select(*CLAVE).withColumn("_en_plata", F.lit(True))
    en_cuarentena = cuarentena.filter(F.col("entidad") == entidad.nombre).select(
        *CLAVE, F.col("motivos").alias("_motivos_actuales"), F.lit(True).alias("_en_cuarentena")
    )
    marcado = (
        bronce.join(en_plata, CLAVE, "left")
        .join(en_cuarentena, CLAVE, "left")
        .withColumn("_en_plata", F.coalesce(F.col("_en_plata"), F.lit(False)))
        .withColumn("_en_cuarentena", F.coalesce(F.col("_en_cuarentena"), F.lit(False)))
        .withColumn("_pendiente", ~F.col("_en_plata") & ~F.col("_en_cuarentena"))
        .withColumn("_id_normalizado", id_normalizado(F.col("id")))
    )
    pendientes = marcado.filter(F.col("_pendiente"))
    ids_nuevos = (
        pendientes.select(F.col("_id_normalizado").alias("_id_nuevo"))
        .filter(F.col("_id_nuevo").isNotNull())
        .distinct()
    )
    contexto = marcado.filter(~F.col("_pendiente")).join(
        ids_nuevos, F.col("_id_normalizado") == F.col("_id_nuevo"), "left_semi"
    )
    evaluadas = evaluar(
        pendientes.unionByName(contexto), entidad, ids_departamentos, ids_cargos
    )

    invalida = F.size(F.col("motivos")) > 0
    motivos_cambian = F.col("_en_cuarentena") & ~F.coalesce(
        F.col("_motivos_actuales") == F.col("motivos"), F.lit(False)
    )
    plata_nuevas = a_plata(evaluadas.filter(F.col("_pendiente") & ~invalida), entidad)
    a_cuarentena_ = a_cuarentena(
        evaluadas.filter(invalida & (F.col("_pendiente") | F.col("_en_plata") | motivos_cambian)),
        entidad,
    )
    retirar = evaluadas.filter(F.col("_en_plata") & invalida).select(*CLAVE)
    # Una fila que ya está en cuarentena no vuelve a plata aunque hoy pasara (D16).
    return Plan(entidad, plata_nuevas, a_cuarentena_, retirar)


# --- Escritura Delta ----------------------------------------------------------------------
def _ddl(esquema) -> str:
    return ", ".join(
        f"`{c.name}` {c.dataType.simpleString()}{'' if c.nullable else ' NOT NULL'}"
        for c in esquema.fields
    )


def crear_tablas_si_no_existen(spark: SparkSession, tablas: Tablas) -> None:
    """Crea plata_* y cuarentena si no existen. No borra ni reemplaza nada."""
    for entidad in (DEPARTAMENTOS, CARGOS, EMPLEADOS):
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {tablas.plata(entidad)} ({_ddl(esquema_plata(entidad))}) "
            f"COMMENT 'Calidad: filas de bronce_{entidad.nombre} que cumplen las reglas, tipadas.'"
        )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {tablas.cuarentena} ({_ddl(ESQUEMA_CUARENTENA)}) "
        "COMMENT 'Calidad: filas crudas rechazadas, tal como llegaron, con todos sus motivos.'"
    )


def leer(spark: SparkSession, tabla: str) -> DataFrame:
    return spark.sql(f"SELECT * FROM {tabla}")


def foto(spark: SparkSession, tabla: str) -> DataFrame:
    """Lectura de la versión actual fijada (time travel), estable entre acciones."""
    version = spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 1").first()["version"]
    return spark.sql(f"SELECT * FROM {tabla} VERSION AS OF {int(version)}")


def _on_clave(t: str = "t", s: str = "s") -> str:
    return " AND ".join(f"{t}.`{c}` = {s}.`{c}`" for c in CLAVE)


def aplicar_plan(spark: SparkSession, plan: Plan, tablas: Tablas) -> dict:
    """Escribe el plan. Cada paso es repetible y solo escribe si tiene filas:

    0. Reconciliar: borra de plata lo que ya esté en cuarentena (deja consistente una
       corrida que se interrumpió entre el paso 1 y el 2).
    1. MERGE en cuarentena: inserta las nuevas; si ya estaba, solo actualiza `motivos`.
    2. Retira de plata las filas que ahora tienen conflicto.
    3. MERGE en plata: inserta solo las claves que no estén.
    """
    plata = tablas.plata(plan.entidad)
    entidad = plan.entidad.nombre
    conteos = {"entidad": entidad}

    en_ambas = foto(spark, plata).join(
        foto(spark, tablas.cuarentena).filter(F.col("entidad") == entidad).select(*CLAVE),
        CLAVE,
        "left_semi",
    )
    conteos["reconciliadas"] = en_ambas.count()
    if conteos["reconciliadas"]:
        en_ambas.select(*CLAVE).createOrReplaceTempView("_calidad_reconciliar")
        spark.sql(
            f"MERGE INTO {plata} t USING _calidad_reconciliar s ON {_on_clave()} "
            "WHEN MATCHED THEN DELETE"
        )

    conteos["cuarentena"] = plan.cuarentena.count()
    if conteos["cuarentena"]:
        plan.cuarentena.createOrReplaceTempView("_calidad_cuarentena")
        spark.sql(
            f"MERGE INTO {tablas.cuarentena} t USING _calidad_cuarentena s "
            f"ON t.entidad = s.entidad AND {_on_clave()} "
            "WHEN MATCHED AND NOT (t.motivos <=> s.motivos) THEN UPDATE SET t.motivos = s.motivos "
            "WHEN NOT MATCHED THEN INSERT *"
        )

    conteos["retiradas"] = plan.retirar.count()
    if conteos["retiradas"]:
        plan.retirar.createOrReplaceTempView("_calidad_retirar")
        spark.sql(
            f"MERGE INTO {plata} t USING _calidad_retirar s ON {_on_clave()} "
            "WHEN MATCHED THEN DELETE"
        )

    conteos["plata"] = plan.plata_nuevas.count()
    if conteos["plata"]:
        plan.plata_nuevas.createOrReplaceTempView("_calidad_plata")
        spark.sql(
            f"MERGE INTO {plata} t USING _calidad_plata s ON {_on_clave()} "
            "WHEN NOT MATCHED THEN INSERT *"
        )
    return conteos


def procesar_entidad(spark: SparkSession, entidad: Entidad, tablas: Tablas) -> dict:
    referencias = {}
    if entidad is EMPLEADOS:
        referencias = {
            "ids_departamentos": foto(spark, tablas.plata(DEPARTAMENTOS)).select("id"),
            "ids_cargos": foto(spark, tablas.plata(CARGOS)).select("id"),
        }
    plan = planificar(
        entidad,
        foto(spark, tablas.bronce(entidad)),
        foto(spark, tablas.plata(entidad)),
        foto(spark, tablas.cuarentena),
        **referencias,
    )
    return aplicar_plan(spark, plan, tablas)


def procesar(spark: SparkSession, tablas: Tablas) -> list[dict]:
    """Corre la calidad completa: dimensiones primero (empleados las referencia)."""
    crear_tablas_si_no_existen(spark, tablas)
    return [procesar_entidad(spark, e, tablas) for e in (DEPARTAMENTOS, CARGOS, EMPLEADOS)]


# --- Verificaciones -----------------------------------------------------------------------
def conciliar(spark: SparkSession, tablas: Tablas) -> list[dict]:
    """Ninguna fila se pierde: cada fila bronce está en plata o en cuarentena, y en una sola."""
    resultado = []
    for entidad in (DEPARTAMENTOS, CARGOS, EMPLEADOS):
        bronce = leer(spark, tablas.bronce(entidad)).select(*CLAVE)
        plata = leer(spark, tablas.plata(entidad)).select(*CLAVE)
        cuarentena = (
            leer(spark, tablas.cuarentena)
            .filter(F.col("entidad") == entidad.nombre)
            .select(*CLAVE)
        )
        resultado.append(
            {
                "entidad": entidad.nombre,
                "bronce": bronce.count(),
                "plata": plata.count(),
                "cuarentena": cuarentena.count(),
                "en_ambas": plata.join(cuarentena, CLAVE, "left_semi").count(),
                "sin_destino": bronce.join(plata.unionByName(cuarentena), CLAVE, "left_anti").count(),
                "huerfanas": plata.unionByName(cuarentena).join(bronce, CLAVE, "left_anti").count(),
            }
        )
    return resultado


def motivos_por_entidad(spark: SparkSession, tablas: Tablas) -> DataFrame:
    """Cuántas filas en cuarentena hay por entidad y motivo (una fila suma en cada motivo)."""
    return (
        leer(spark, tablas.cuarentena)
        .select("entidad", F.explode("motivos").alias("motivo"))
        .groupBy("entidad", "motivo")
        .count()
        .orderBy("entidad", "motivo")
    )


def avisos_dimensiones(spark: SparkSession, tablas: Tablas) -> dict[str, DataFrame]:
    """D12: nombres repetidos con ids distintos en las dimensiones (solo se reportan)."""
    return {
        "departamentos": nombres_repetidos(
            leer(spark, tablas.plata(DEPARTAMENTOS)), "departamento"
        ),
        "cargos": nombres_repetidos(leer(spark, tablas.plata(CARGOS)), "cargo"),
    }
