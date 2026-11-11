"""Planificación incremental (D8): qué se procesa y la unicidad contra la historia.

Prueba `calidad.incremental.planificar`, que es puro: recibe el estado de plata y
cuarentena como DataFrames. El estado "después del lote 1" es exactamente lo que la
primera corrida escribe sobre tablas vacías: las salidas del primer plan (no hay nada
que actualizar ni retirar). La escritura Delta (MERGE) se prueba aparte, con tablas.
"""

from pyspark.sql import functions as F

from calidad import motivos as M
from calidad.incremental import planificar
from calidad.reglas import ESQUEMA_CUARENTENA, EMPLEADOS, esquema_plata
from fabricas import (
    INGESTA_1,
    INGESTA_2,
    LOTE_1,
    LOTE_2,
    VALIDA,
    bronce,
    claves,
    ids,
    motivos_por_clave,
)


def con_id(id_, **cambios):
    columnas = dict(zip(EMPLEADOS.columnas, VALIDA))
    columnas["id"] = id_
    columnas.update(cambios)
    return tuple(columnas[c] for c in EMPLEADOS.columnas)


def planear(spark, bronce_df, plata=None, cuarentena=None):
    plata = plata if plata is not None else spark.createDataFrame([], esquema_plata(EMPLEADOS))
    cuarentena = cuarentena if cuarentena is not None else spark.createDataFrame([], ESQUEMA_CUARENTENA)
    return planificar(EMPLEADOS, bronce_df, plata, cuarentena, ids(spark, [1, 2]), ids(spark, [1, 2]))


# Lote 1 de prueba: 1 válida, 2 válidas, 3 sin nombre, 4 copia exacta de la 1.
FILAS_LOTE_1 = [con_id("1"), con_id("2"), con_id("3", nombre=""), con_id("1")]


def lote_1(spark):
    return bronce(spark, EMPLEADOS, FILAS_LOTE_1, archivo=LOTE_1, ingestado=INGESTA_1)


def lote_2(spark, filas):
    return bronce(spark, EMPLEADOS, filas, archivo=LOTE_2, ingestado=INGESTA_2)


def test_primera_corrida_reparte_todas_las_filas_sin_perder_ninguna(spark):
    plan = planear(spark, lote_1(spark))
    en_plata, en_cuarentena = claves(plan.plata_nuevas), claves(plan.cuarentena)
    assert en_plata == {(LOTE_1, 1), (LOTE_1, 2)}
    assert motivos_por_clave(plan.cuarentena) == {
        (LOTE_1, 3): [M.campo_vacio("nombre")],
        (LOTE_1, 4): [M.ID_DUPLICADO_COPIA],
    }
    assert en_plata.isdisjoint(en_cuarentena)
    assert len(en_plata) + len(en_cuarentena) == 4  # ninguna fila cruda se pierde
    assert plan.retirar.count() == 0


def test_la_cuarentena_guarda_la_fila_tal_como_llego(spark):
    filas = [con_id("5", nombre="  Ana Pérez ", id_departamento="99")]
    plan = planear(spark, bronce(spark, EMPLEADOS, filas))
    r = plan.cuarentena.first()
    assert r["entidad"] == "empleados"
    assert r["_linea_original"] == "5,  Ana Pérez ,2021-07-27T16:02:08Z,99,1"
    assert r["id_original"] == "5"
    assert r["valores_originales"]["nombre"] == "  Ana Pérez "  # sin recortar
    assert list(r["motivos"]) == [M.DEPARTAMENTO_NO_EXISTE]
    assert r["rechazado_en"] is not None


def test_plata_queda_tipada_y_con_sus_correcciones(spark):
    plan = planear(spark, bronce(spark, EMPLEADOS, [con_id(" 6 ", nombre=" Luis Gómez")]))
    # Mismos nombres y tipos que la tabla plata (la nulabilidad la impone la tabla).
    assert [(f.name, f.dataType) for f in plan.plata_nuevas.schema.fields] == [
        (f.name, f.dataType) for f in esquema_plata(EMPLEADOS).fields
    ]
    r = plan.plata_nuevas.first()
    assert (r["id"], r["nombre"]) == (6, "Luis Gómez")
    assert list(r["_correcciones"]) == [M.recorte_espacios("id"), M.recorte_espacios("nombre")]


def test_reprocesar_el_mismo_lote_no_produce_cambios(spark):
    primero = planear(spark, lote_1(spark))
    segundo = planear(spark, lote_1(spark), primero.plata_nuevas, primero.cuarentena)
    assert segundo.plata_nuevas.count() == 0
    assert segundo.cuarentena.count() == 0
    assert segundo.retirar.count() == 0


def test_lote_2_con_copia_exacta_de_una_fila_plata(spark):
    primero = planear(spark, lote_1(spark))
    todo = lote_1(spark).unionByName(lote_2(spark, [con_id("2"), con_id("20")]))
    plan = planear(spark, todo, primero.plata_nuevas, primero.cuarentena)
    assert claves(plan.plata_nuevas) == {(LOTE_2, 2)}  # solo el id 20, nuevo
    assert motivos_por_clave(plan.cuarentena) == {(LOTE_2, 1): [M.ID_DUPLICADO_COPIA]}
    assert plan.retirar.count() == 0  # el id 2 del lote 1 sigue en plata


def test_lote_2_con_otra_version_de_un_id_plata_retira_ambas(spark):
    primero = planear(spark, lote_1(spark))
    otra_version = con_id("2", fecha_contratacion="2022-01-10T08:00:00Z")
    todo = lote_1(spark).unionByName(lote_2(spark, [otra_version]))
    plan = planear(spark, todo, primero.plata_nuevas, primero.cuarentena)
    assert plan.plata_nuevas.count() == 0
    assert motivos_por_clave(plan.cuarentena) == {
        (LOTE_2, 1): [M.ID_DUPLICADO_CONFLICTO],
        (LOTE_1, 2): [M.ID_DUPLICADO_CONFLICTO],  # la fila plata del lote 1 se retira
    }
    assert claves(plan.retirar) == {(LOTE_1, 2)}


def test_lote_2_choca_con_un_id_que_ya_estaba_en_cuarentena(spark):
    primero = planear(spark, lote_1(spark))  # el id 3 llegó sin nombre en el lote 1
    todo = lote_1(spark).unionByName(lote_2(spark, [con_id("3")]))
    plan = planear(spark, todo, primero.plata_nuevas, primero.cuarentena)
    assert plan.plata_nuevas.count() == 0
    assert motivos_por_clave(plan.cuarentena) == {
        (LOTE_2, 1): [M.ID_DUPLICADO_CONFLICTO],
        # la fila del lote 1 gana el motivo nuevo (se actualiza, no se duplica)
        (LOTE_1, 3): [M.campo_vacio("nombre"), M.ID_DUPLICADO_CONFLICTO],
    }
    assert plan.retirar.count() == 0


def test_solo_se_reevaluan_las_filas_nuevas_y_las_de_sus_ids(spark):
    primero = planear(spark, lote_1(spark))
    todo = lote_1(spark).unionByName(lote_2(spark, [con_id("30"), con_id("31", id_cargo="")]))
    plan = planear(spark, todo, primero.plata_nuevas, primero.cuarentena)
    tocadas = claves(plan.plata_nuevas) | claves(plan.cuarentena) | claves(plan.retirar)
    assert tocadas == {(LOTE_2, 1), (LOTE_2, 2)}
    assert all(archivo == LOTE_2 for archivo, _ in tocadas)
    assert plan.cuarentena.filter(F.col("_archivo_origen") == LOTE_1).count() == 0
