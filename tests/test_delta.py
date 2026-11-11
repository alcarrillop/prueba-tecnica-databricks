"""Calidad de punta a punta sobre tablas Delta reales (escritura, repetición e incremental).

Cada prueba crea sus propias tablas en el esquema del proyecto con el prefijo
`test_<sufijo único>_` (bronce, plata y cuarentena), corre `calidad.incremental.procesar`
(el mismo código que ejecuta el job) y borra al final solo las tablas que creó.

Marcadas como `tablas`: `uv run pytest -m "not tablas"` corre solo las de memoria.
"""

import pytest
from pyspark.sql import types as T

from calidad import motivos as M
from calidad.incremental import (
    Plan,
    Tablas,
    aplicar_plan,
    conciliar,
    foto,
    leer,
    planificar,
    procesar,
)
from calidad.reglas import CARGOS, DEPARTAMENTOS, EMPLEADOS, ENTIDADES
from fabricas import (
    CATALOGO,
    ESQUEMA,
    INGESTA_1,
    INGESTA_2,
    LANDING,
    LOTE_1,
    LOTE_2,
    VALIDA,
    bronce,
    esquema_bronce,
)

pytestmark = pytest.mark.tablas

DEPARTAMENTOS_CSV = [("1", "Ventas"), ("2", "  Talento Humano"), ("3", "Tecnología ")]
CARGOS_CSV = [("1", "Gerente"), ("2", "Analista de Datos")]


def con_id(id_, **cambios):
    columnas = dict(zip(EMPLEADOS.columnas, VALIDA))
    columnas["id"] = id_
    columnas.update(cambios)
    return tuple(columnas[c] for c in EMPLEADOS.columnas)


# Lote 1: ids 1, 2, 5 válidos; 3 sin nombre; 4 con departamento 99; copia exacta del 1;
# id 6 en conflicto consigo mismo (dos versiones en el mismo lote).
FILAS_LOTE_1 = [
    con_id("1"),
    con_id("2", nombre="Luis Gómez"),
    con_id("3", nombre=""),
    con_id("4", id_departamento="99"),
    con_id("1"),
    con_id("5", fecha_contratacion="2021-12-31T23:59:59Z", id_departamento="3"),
    con_id("6", fecha_contratacion="2020-03-23T03:31:10Z"),
    con_id("6", fecha_contratacion="2020-08-26T14:30:28Z"),
]
# Lote 2: copia exacta del 1 (plata), otra versión del 2 (plata -> se retira), el 3
# completo (estaba en cuarentena), y el 7 nuevo y válido.
FILAS_LOTE_2 = [
    con_id("1"),
    con_id("2", nombre="Luis Gómez", id_cargo="2"),
    con_id("3"),
    con_id("7", nombre="Sara Vélez"),
]


@pytest.fixture
def nuevas_tablas(spark, prefijo_temporal):
    """Fábrica de juegos de tablas temporales (bronce vacías); la limpieza la hace
    `prefijo_temporal`, que borra solo lo que tenga su prefijo único."""

    def crear():
        tablas = Tablas(CATALOGO, ESQUEMA, prefijo_temporal())
        for entidad in ENTIDADES.values():
            spark.sql(f"CREATE TABLE {tablas.bronce(entidad)} ({esquema_bronce(entidad)})")
        return tablas

    return crear


def agregar_bronce(spark, tablas, entidad, filas, archivo, ingestado):
    """Simula una carga de ingesta: agrega filas (append-only) a la tabla bronce."""
    bronce(spark, entidad, filas, archivo=archivo, ingestado=ingestado).write.mode(
        "append"
    ).saveAsTable(tablas.bronce(entidad))


def cargar_dimensiones(spark, tablas):
    agregar_bronce(spark, tablas, DEPARTAMENTOS, DEPARTAMENTOS_CSV,
                   f"{LANDING}/departamentos/departamentos.csv", INGESTA_1)
    agregar_bronce(spark, tablas, CARGOS, CARGOS_CSV, f"{LANDING}/cargos/cargos.csv", INGESTA_1)


def versiones(spark, tablas):
    """Última versión Delta de cada tabla que escribe calidad."""
    nombres = [tablas.plata(e) for e in ENTIDADES.values()] + [tablas.cuarentena]
    return {n: spark.sql(f"DESCRIBE HISTORY {n} LIMIT 1").first()["version"] for n in nombres}


def motivos_empleados(spark, tablas):
    filas = spark.sql(
        f"SELECT _archivo_origen, _numero_linea, motivos FROM {tablas.cuarentena} "
        "WHERE entidad = 'empleados'"
    ).collect()
    return {(r[0], r[1]): list(r[2]) for r in filas}


def ids_plata(spark, tablas):
    return sorted(r["id"] for r in spark.sql(f"SELECT id FROM {tablas.plata(EMPLEADOS)}").collect())


def consulta_comparable(spark, tabla, version=None, excluir=()):
    """SELECT de la tabla apto para EXCEPT ALL: Spark no compara columnas MAP en
    operaciones de conjuntos, así que el mapa se compara como arreglo ordenado de pares."""
    columnas = []
    for campo in leer(spark, tabla).schema.fields:
        if campo.name in excluir:
            continue
        if isinstance(campo.dataType, T.MapType):
            columnas.append(f"array_sort(map_entries(`{campo.name}`)) AS `{campo.name}`")
        else:
            columnas.append(f"`{campo.name}`")
    origen = tabla if version is None else f"{tabla} VERSION AS OF {version}"
    return f"SELECT {', '.join(columnas)} FROM {origen}"


def filas_distintas(spark, consulta_a, consulta_b):
    """Filas que están en una consulta y no en la otra (en ambos sentidos, con repetidas)."""
    return spark.sql(
        f"({consulta_a} EXCEPT ALL {consulta_b}) UNION ALL ({consulta_b} EXCEPT ALL {consulta_a})"
    ).count()


def conciliacion_ok(spark, tablas):
    for fila in conciliar(spark, tablas):
        assert fila["sin_destino"] == 0, fila
        assert fila["en_ambas"] == 0, fila
        assert fila["huerfanas"] == 0, fila
        assert fila["bronce"] == fila["plata"] + fila["cuarentena"], fila


# --- Pruebas -------------------------------------------------------------------------------
def test_primera_corrida_escribe_plata_y_cuarentena_sin_perder_filas(spark, nuevas_tablas):
    tablas = nuevas_tablas()
    cargar_dimensiones(spark, tablas)
    agregar_bronce(spark, tablas, EMPLEADOS, FILAS_LOTE_1, LOTE_1, INGESTA_1)

    procesar(spark, tablas)

    conciliacion_ok(spark, tablas)
    assert ids_plata(spark, tablas) == [1, 2, 5]
    assert motivos_empleados(spark, tablas) == {
        (LOTE_1, 3): [M.campo_vacio("nombre")],
        (LOTE_1, 4): [M.DEPARTAMENTO_NO_EXISTE],
        (LOTE_1, 5): [M.ID_DUPLICADO_COPIA],
        (LOTE_1, 7): [M.ID_DUPLICADO_CONFLICTO],
        (LOTE_1, 8): [M.ID_DUPLICADO_CONFLICTO],
    }
    departamentos = {
        r["id"]: (r["departamento"], list(r["_correcciones"]))
        for r in spark.sql(f"SELECT * FROM {tablas.plata(DEPARTAMENTOS)}").collect()
    }
    assert departamentos == {
        1: ("Ventas", []),
        2: ("Talento Humano", [M.recorte_espacios("departamento")]),
        3: ("Tecnología", [M.recorte_espacios("departamento")]),
    }
    # El borde del año queda como instante UTC en plata.
    r = spark.sql(
        f"SELECT unix_micros(fecha_contratacion) AS m FROM {tablas.plata(EMPLEADOS)} WHERE id = 5"
    ).first()
    assert r["m"] == 1640995199 * 1_000_000  # 2021-12-31T23:59:59Z


def test_repetir_la_corrida_no_escribe_nada(spark, nuevas_tablas):
    tablas = nuevas_tablas()
    cargar_dimensiones(spark, tablas)
    agregar_bronce(spark, tablas, EMPLEADOS, FILAS_LOTE_1, LOTE_1, INGESTA_1)
    procesar(spark, tablas)
    antes = versiones(spark, tablas)

    escrituras = procesar(spark, tablas)

    assert versiones(spark, tablas) == antes  # ninguna versión Delta nueva
    assert all(e["plata"] == e["cuarentena"] == e["retiradas"] == 0 for e in escrituras)
    for nombre, version in antes.items():
        anterior = consulta_comparable(spark, nombre, version)
        assert filas_distintas(spark, anterior, consulta_comparable(spark, nombre)) == 0, nombre


def test_lote_2_se_revisa_contra_toda_la_historia(spark, nuevas_tablas):
    tablas = nuevas_tablas()
    cargar_dimensiones(spark, tablas)
    agregar_bronce(spark, tablas, EMPLEADOS, FILAS_LOTE_1, LOTE_1, INGESTA_1)
    procesar(spark, tablas)
    rechazo_id_3 = spark.sql(
        f"SELECT rechazado_en FROM {tablas.cuarentena} WHERE _numero_linea = 3 "
        f"AND _archivo_origen = '{LOTE_1}'"
    ).first()["rechazado_en"]

    agregar_bronce(spark, tablas, EMPLEADOS, FILAS_LOTE_2, LOTE_2, INGESTA_2)
    escrituras = procesar(spark, tablas)

    conciliacion_ok(spark, tablas)
    assert ids_plata(spark, tablas) == [1, 5, 7]  # el 2 se retiró; entró el 7
    assert motivos_empleados(spark, tablas) == {
        (LOTE_1, 2): [M.ID_DUPLICADO_CONFLICTO],  # retirada de plata
        (LOTE_1, 3): [M.campo_vacio("nombre"), M.ID_DUPLICADO_CONFLICTO],  # ganó un motivo
        (LOTE_1, 4): [M.DEPARTAMENTO_NO_EXISTE],
        (LOTE_1, 5): [M.ID_DUPLICADO_COPIA],
        (LOTE_1, 7): [M.ID_DUPLICADO_CONFLICTO],
        (LOTE_1, 8): [M.ID_DUPLICADO_CONFLICTO],
        (LOTE_2, 1): [M.ID_DUPLICADO_COPIA],
        (LOTE_2, 2): [M.ID_DUPLICADO_CONFLICTO],
        (LOTE_2, 3): [M.ID_DUPLICADO_CONFLICTO],
    }
    # La fila del id 3 se actualizó en su lugar: mismo momento de rechazo, sin duplicarse.
    assert spark.sql(
        f"SELECT rechazado_en FROM {tablas.cuarentena} WHERE _numero_linea = 3 "
        f"AND _archivo_origen = '{LOTE_1}'"
    ).first()["rechazado_en"] == rechazo_id_3
    empleados = next(e for e in escrituras if e["entidad"] == "empleados")
    assert (empleados["plata"], empleados["retiradas"]) == (1, 1)

    antes = versiones(spark, tablas)
    procesar(spark, tablas)  # repetir con el lote 2 ya procesado
    assert versiones(spark, tablas) == antes


def test_procesar_por_lotes_da_lo_mismo_que_todo_junto(spark, nuevas_tablas):
    por_lotes, junto = nuevas_tablas(), nuevas_tablas()
    for tablas in (por_lotes, junto):
        cargar_dimensiones(spark, tablas)
        agregar_bronce(spark, tablas, EMPLEADOS, FILAS_LOTE_1, LOTE_1, INGESTA_1)
    procesar(spark, por_lotes)
    for tablas in (por_lotes, junto):
        agregar_bronce(spark, tablas, EMPLEADOS, FILAS_LOTE_2, LOTE_2, INGESTA_2)
    procesar(spark, por_lotes)
    procesar(spark, junto)

    # Se comparan todas las columnas salvo los momentos de proceso y de rechazo.
    pares = [(por_lotes.plata(e), junto.plata(e), "_procesado_en") for e in ENTIDADES.values()]
    pares.append((por_lotes.cuarentena, junto.cuarentena, "rechazado_en"))
    for a, b, momento in pares:
        consulta_a = consulta_comparable(spark, a, excluir=(momento,))
        consulta_b = consulta_comparable(spark, b, excluir=(momento,))
        assert spark.sql(consulta_a).count() > 0, a
        assert filas_distintas(spark, consulta_a, consulta_b) == 0, (a, b)


def test_una_corrida_interrumpida_se_corrige_en_la_siguiente(spark, nuevas_tablas):
    tablas = nuevas_tablas()
    cargar_dimensiones(spark, tablas)
    agregar_bronce(spark, tablas, EMPLEADOS, FILAS_LOTE_1, LOTE_1, INGESTA_1)
    procesar(spark, tablas)
    agregar_bronce(spark, tablas, EMPLEADOS, FILAS_LOTE_2, LOTE_2, INGESTA_2)

    # Corrida interrumpida: solo alcanzó a escribir la cuarentena (paso 1 de aplicar_plan).
    plan = planificar(
        EMPLEADOS,
        foto(spark, tablas.bronce(EMPLEADOS)),
        foto(spark, tablas.plata(EMPLEADOS)),
        foto(spark, tablas.cuarentena),
        foto(spark, tablas.plata(DEPARTAMENTOS)).select("id"),
        foto(spark, tablas.plata(CARGOS)).select("id"),
    )
    vacio = plan.retirar.limit(0)
    aplicar_plan(spark, Plan(EMPLEADOS, plan.plata_nuevas.limit(0), plan.cuarentena, vacio), tablas)
    en_ambas = next(f for f in conciliar(spark, tablas) if f["entidad"] == "empleados")["en_ambas"]
    assert en_ambas == 1  # el id 2 del lote 1 quedó en plata y en cuarentena

    procesar(spark, tablas)

    conciliacion_ok(spark, tablas)
    assert ids_plata(spark, tablas) == [1, 5, 7]
    assert motivos_empleados(spark, tablas)[(LOTE_1, 2)] == [M.ID_DUPLICADO_CONFLICTO]


def test_fila_en_cuarentena_conserva_la_linea_tal_como_llego(spark, nuevas_tablas):
    tablas = nuevas_tablas()
    cargar_dimensiones(spark, tablas)
    agregar_bronce(spark, tablas, EMPLEADOS, [con_id("9", nombre="  ", id_cargo="x")], LOTE_1, INGESTA_1)
    procesar(spark, tablas)
    r = spark.sql(f"SELECT * FROM {tablas.cuarentena}").first()
    assert r["_linea_original"] == "9,  ,2021-07-27T16:02:08Z,1,x"
    assert r["valores_originales"]["nombre"] == "  "
    assert r["id_original"] == "9"
    assert list(r["motivos"]) == [M.campo_vacio("nombre"), M.tipo_invalido("id_cargo")]
    assert spark.sql(f"SELECT count(*) AS n FROM {tablas.plata(EMPLEADOS)}").first()["n"] == 0
    assert leer(spark, tablas.cuarentena).filter("entidad <> 'empleados'").count() == 0
