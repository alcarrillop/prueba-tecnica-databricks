"""Métricas de la Sección 3 contra el código real de sql (`metricas.ejecutar.ejecutar`).

Cada prueba arma tablas plata pequeñas, hechas a mano, con un prefijo único
(`test_<sufijo>_plata_*`), corre los archivos SQL de las métricas con ese prefijo (escriben
`test_<sufijo>_oro_*` y sus vistas `_ordenada`) y compara con el resultado conocido. Al final
se borra todo lo que tenga el prefijo.

Las fechas se arman en SQL a partir del texto ISO con Z (instantes UTC, sin depender de la
zona de la sesión) y los resultados se comparan como conteos enteros: Databricks Connect
devuelve los TIMESTAMP como datetime en la hora local del equipo, así que no se comparan
fechas en Python.
"""

import pytest
from pyspark.sql import functions as F

from calidad.incremental import Tablas, crear_tablas_si_no_existen, procesar
from calidad.reglas import CARGOS, DEPARTAMENTOS, EMPLEADOS, ENTIDADES, FORMATO_FECHA, esquema_plata
from fabricas import CATALOGO, ESQUEMA, INGESTA_1, LANDING, LOTE_1, bronce, esquema_bronce
from metricas.ejecutar import METRICA_1, METRICA_2, TABLA_METRICA_1, TABLA_METRICA_2, SUFIJO_VISTA, ejecutar, verificar

pytestmark = pytest.mark.tablas

COLUMNAS_METRICA_1 = ["departamento", "cargo", "Q1", "Q2", "Q3", "Q4"]
COLUMNAS_METRICA_2 = ["id", "departamento", "contratados"]


# --- Ayudas --------------------------------------------------------------------------------
def _escribir(spark, df, tabla, entidad):
    columnas = [c.name for c in esquema_plata(entidad).fields]
    df.select(*columnas).write.mode("append").saveAsTable(tabla)


def crear_plata(spark, prefijo, departamentos, cargos, empleados):
    """Tablas plata con el esquema real de calidad, llenas a mano.

    departamentos y cargos: [(id, nombre)]. empleados: [(id, fecha ISO con Z, id_dep, id_cargo)].
    """
    tablas = Tablas(CATALOGO, ESQUEMA, prefijo)
    crear_tablas_si_no_existen(spark, tablas)
    meta = [
        F.lit("prueba.csv").alias("_archivo_origen"),
        F.col("id").alias("_numero_linea"),
        F.array().cast("array<string>").alias("_correcciones"),
        F.current_timestamp().alias("_ingestado_en"),
        F.current_timestamp().alias("_procesado_en"),
    ]
    for entidad, filas, columna in (
        (DEPARTAMENTOS, departamentos, "departamento"),
        (CARGOS, cargos, "cargo"),
    ):
        df = spark.createDataFrame(filas, f"id INT, {columna} STRING").select("*", *meta)
        _escribir(spark, df, tablas.plata(entidad), entidad)
    df = spark.createDataFrame(
        empleados, "id INT, fecha STRING, id_departamento INT, id_cargo INT"
    ).select(
        "id",
        F.concat(F.lit("Empleado "), F.col("id").cast("string")).alias("nombre"),
        F.try_to_timestamp(F.col("fecha"), F.lit(FORMATO_FECHA)).alias("fecha_contratacion"),
        "id_departamento",
        "id_cargo",
        *meta,
    )
    _escribir(spark, df, tablas.plata(EMPLEADOS), EMPLEADOS)
    return tablas


def correr_metricas(spark, prefijo):
    ejecutar(spark, METRICA_1, CATALOGO, ESQUEMA, prefijo)
    ejecutar(spark, METRICA_2, CATALOGO, ESQUEMA, prefijo)


def leer(spark, prefijo, objeto):
    """(columnas, filas como tuplas en el orden en que llegan) de una tabla o vista oro."""
    df = spark.sql(f"SELECT * FROM `{CATALOGO}`.`{ESQUEMA}`.`{prefijo}{objeto}`")
    return df.columns, [tuple(r) for r in df.collect()]


def contrataciones(n, fecha, id_dep, id_cargo, desde):
    """n empleados con ids consecutivos desde `desde`, misma fecha, departamento y cargo."""
    return [(desde + i, fecha, id_dep, id_cargo) for i in range(n)]


# --- Métrica 1 -----------------------------------------------------------------------------
# Bordes de año y trimestre en UTC. Los ids 1237, 1221 y 1096 son filas reales del lote 1:
# en Bogotá (UTC-5) el 1237 caería en 2020, el 1221 en Q1 y el 1096 dentro de 2021.
BORDES_UTC = [
    (101, "2021-01-01T00:00:00Z", 1, 2),  # Q1, primer instante de 2021
    (1237, "2021-01-01T04:23:19Z", 1, 2),  # Q1
    (103, "2021-03-31T23:59:59Z", 1, 2),  # Q1, último instante
    (104, "2021-04-01T00:00:00Z", 1, 2),  # Q2
    (1221, "2021-04-01T01:19:11Z", 1, 1),  # Q2
    (106, "2021-06-30T23:59:59Z", 1, 1),  # Q2
    (107, "2021-07-01T00:00:00Z", 1, 1),  # Q3
    (108, "2021-09-30T23:59:59Z", 1, 1),  # Q3
    (109, "2021-10-01T00:00:00Z", 1, 1),  # Q4
    (110, "2021-11-07T06:30:00Z", 1, 1),  # Q4, hora repetida del cambio de horario en Nueva York
    (111, "2021-12-31T23:59:59Z", 1, 1),  # Q4, último instante de 2021
    (112, "2020-12-31T23:59:59Z", 1, 1),  # fuera: 2020
    (1096, "2022-01-01T01:47:18Z", 1, 1),  # fuera: 2022
    (114, "2020-06-15T12:00:00Z", 2, 3),  # Compras solo contrató en 2020
]


@pytest.mark.parametrize("zona", ["UTC", "America/Bogota", "America/New_York"])
def test_anio_y_trimestre_se_calculan_en_utc_con_cualquier_zona_de_sesion(spark, prefijo_temporal, zona):
    prefijo = prefijo_temporal()
    anterior = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", zona)
    try:
        crear_plata(
            spark,
            prefijo,
            [(1, "Ventas"), (2, "Compras")],
            [(1, "Analista Financiero"), (2, "Analista de Datos"), (3, "Gerente")],
            BORDES_UTC,
        )
        correr_metricas(spark, prefijo)
        columnas, filas = leer(spark, prefijo, TABLA_METRICA_1 + SUFIJO_VISTA)
        columnas_2, filas_2 = leer(spark, prefijo, TABLA_METRICA_2 + SUFIJO_VISTA)
    finally:
        spark.conf.set("spark.sql.session.timeZone", anterior)

    assert columnas == COLUMNAS_METRICA_1
    assert filas == [
        ("Ventas", "Analista de Datos", 3, 1, 0, 0),
        ("Ventas", "Analista Financiero", 0, 2, 2, 3),
    ]
    # Q1 + Q2 + Q3 + Q4 = total de contrataciones de 2021 (11 por construcción).
    assert sum(sum(f[2:]) for f in filas) == 11
    # Métrica 2 con los mismos datos: promedio 11 / 2 departamentos = 5.5.
    assert columnas_2 == COLUMNAS_METRICA_2
    assert filas_2 == [(1, "Ventas", 11)]


def test_metrica_1_solo_combinaciones_con_contrataciones_en_orden_del_espanol(spark, prefijo_temporal):
    prefijo = prefijo_temporal()
    crear_plata(
        spark,
        prefijo,
        [(1, "Ventas"), (2, "Área Legal"), (3, "Auditoría"), (4, "Compras")],
        [(1, "Analista Financiero"), (2, "Analista de Datos"), (3, "Técnico"), (4, "Tesorero"), (5, "Gerente")],
        [
            (1, "2021-02-01T10:00:00Z", 1, 4),  # Ventas / Tesorero Q1
            (2, "2021-08-01T10:00:00Z", 1, 3),  # Ventas / Técnico Q3
            (3, "2021-05-01T10:00:00Z", 1, 1),  # Ventas / Analista Financiero Q2
            (4, "2021-11-01T10:00:00Z", 1, 2),  # Ventas / Analista de Datos Q4
            (5, "2021-05-10T10:00:00Z", 2, 3),  # Área Legal / Técnico Q2
            (6, "2021-02-10T10:00:00Z", 3, 4),  # Auditoría / Tesorero Q1
            (7, "2021-03-10T10:00:00Z", 3, 4),  # Auditoría / Tesorero Q1
            (8, "2020-06-01T10:00:00Z", 4, 5),  # Compras / Gerente: 2020, no aparece
        ],
    )
    correr_metricas(spark, prefijo)
    esperado = [
        ("Área Legal", "Técnico", 0, 1, 0, 0),
        ("Auditoría", "Tesorero", 2, 0, 0, 0),
        ("Ventas", "Analista de Datos", 0, 0, 0, 1),  # antes que Analista Financiero
        ("Ventas", "Analista Financiero", 0, 1, 0, 0),
        ("Ventas", "Técnico", 0, 0, 1, 0),  # antes que Tesorero (sin importar la tilde)
        ("Ventas", "Tesorero", 1, 0, 0, 0),
    ]
    columnas, vista = leer(spark, prefijo, TABLA_METRICA_1 + SUFIJO_VISTA)
    assert columnas == COLUMNAS_METRICA_1
    assert vista == esperado
    columnas_tabla, tabla = leer(spark, prefijo, TABLA_METRICA_1)
    assert columnas_tabla == COLUMNAS_METRICA_1
    assert sorted(tabla) == sorted(esperado)

    # Reconstruirla con el mismo plata da exactamente lo mismo.
    correr_metricas(spark, prefijo)
    assert leer(spark, prefijo, TABLA_METRICA_1 + SUFIJO_VISTA)[1] == esperado


# --- Métrica 2 -----------------------------------------------------------------------------
def test_metrica_2_promedio_incluye_departamentos_sin_contrataciones_y_es_estricto(spark, prefijo_temporal):
    prefijo = prefijo_temporal()
    # 2021: Compras 6, Área Legal 6, Auditoría 6, Ventas 7, Legal 5, Mercadeo 0 -> 30 / 6 = 5.
    # Legal (5) queda justo en el promedio: no entra. Sin contar a Mercadeo el promedio
    # sería 6 y solo entraría Ventas.
    empleados = (
        contrataciones(6, "2021-03-01T10:00:00Z", 1, 1, desde=100)
        + contrataciones(6, "2021-06-01T10:00:00Z", 2, 1, desde=200)
        + contrataciones(6, "2021-09-01T10:00:00Z", 3, 1, desde=300)
        + contrataciones(7, "2021-12-01T10:00:00Z", 4, 1, desde=400)
        + contrataciones(5, "2021-01-15T10:00:00Z", 5, 1, desde=500)
        + contrataciones(3, "2020-05-01T10:00:00Z", 6, 1, desde=600)  # Mercadeo: solo 2020
        + contrataciones(2, "2022-02-01T10:00:00Z", 5, 1, desde=700)  # Legal en 2022: no cuenta
    )
    crear_plata(
        spark,
        prefijo,
        [(1, "Compras"), (2, "Área Legal"), (3, "Auditoría"), (4, "Ventas"), (5, "Legal"), (6, "Mercadeo")],
        [(1, "Gerente")],
        empleados,
    )
    correr_metricas(spark, prefijo)
    columnas, vista = leer(spark, prefijo, TABLA_METRICA_2 + SUFIJO_VISTA)
    assert columnas == COLUMNAS_METRICA_2
    # Mayor a menor; el empate en 6 se resuelve por nombre en orden del español.
    assert vista == [(4, "Ventas", 7), (2, "Área Legal", 6), (3, "Auditoría", 6), (1, "Compras", 6)]
    assert sorted(leer(spark, prefijo, TABLA_METRICA_2)[1]) == sorted(vista)


def test_metrica_2_desempate_final_por_id(spark, prefijo_temporal):
    prefijo = prefijo_temporal()
    empleados = (
        contrataciones(2, "2021-04-01T10:00:00Z", 8, 1, desde=800)
        + contrataciones(2, "2021-04-01T10:00:00Z", 3, 1, desde=300)
    )
    crear_plata(spark, prefijo, [(8, "Finanzas"), (3, "Finanzas"), (5, "Legal")], [(1, "Gerente")], empleados)
    correr_metricas(spark, prefijo)
    # Promedio 4 / 3; los dos Finanzas empatan en contratados y en nombre: decide el id.
    assert leer(spark, prefijo, TABLA_METRICA_2 + SUFIJO_VISTA)[1] == [(3, "Finanzas", 2), (8, "Finanzas", 2)]


# --- De punta a punta: bronce -> calidad -> oro --------------------------------------------
def test_las_filas_en_cuarentena_no_cuentan_en_las_metricas(spark, prefijo_temporal):
    tablas = Tablas(CATALOGO, ESQUEMA, prefijo_temporal())
    for entidad in ENTIDADES.values():
        spark.sql(f"CREATE TABLE {tablas.bronce(entidad)} ({esquema_bronce(entidad)})")

    def agregar(entidad, filas, archivo):
        bronce(spark, entidad, filas, archivo=archivo, ingestado=INGESTA_1).write.mode(
            "append"
        ).saveAsTable(tablas.bronce(entidad))

    agregar(DEPARTAMENTOS, [("1", "Ventas"), ("2", " Compras ")], f"{LANDING}/departamentos/d.csv")
    agregar(CARGOS, [("1", "Gerente"), ("2", "Analista de Datos")], f"{LANDING}/cargos/c.csv")
    agregar(
        EMPLEADOS,
        [
            ("1", "Ana Pérez", "2021-02-01T10:00:00Z", "1", "1"),  # Q1
            ("2", "Luis Gómez", "2021-05-01T10:00:00Z", "2", "2"),  # Q2
            ("1", "Ana Pérez", "2021-02-01T10:00:00Z", "1", "1"),  # copia exacta: cuenta una vez
            ("3", "Sara Vélez", "2021-03-01T10:00:00Z", "99", "1"),  # departamento inexistente
            ("4", "", "2021-08-01T10:00:00Z", "1", "1"),  # sin nombre
            ("5", "Juan Ruiz", "ayer", "1", "1"),  # fecha inválida
            ("6", "Eva Mora", "2021-11-30T23:00:00Z", "1", "1"),  # Q4
        ],
        LOTE_1,
    )
    procesar(spark, tablas)
    correr_metricas(spark, tablas.prefijo)

    assert leer(spark, tablas.prefijo, TABLA_METRICA_1 + SUFIJO_VISTA)[1] == [
        ("Compras", "Analista de Datos", 0, 1, 0, 0),  # nombre ya recortado por calidad
        ("Ventas", "Gerente", 1, 0, 0, 1),
    ]
    # 3 contrataciones válidas en 2021 entre 2 departamentos: promedio 1.5.
    assert leer(spark, tablas.prefijo, TABLA_METRICA_2 + SUFIJO_VISTA)[1] == [(1, "Ventas", 2)]
    # La verificación de sql (cálculo alterno y suma de trimestres) también pasa aquí.
    resumen = verificar(spark, CATALOGO, ESQUEMA, tablas.prefijo)
    assert (resumen["contratados_2021"], resumen["departamentos"]) == (3, 2)
