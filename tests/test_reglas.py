"""Reglas de calidad (Sección 2) sobre DataFrames en memoria, con resultados conocidos.

Cada prueba arma filas crudas a mano, llama a `calidad.reglas.evaluar` (el código real
del pipeline) y compara los motivos y valores con lo esperado.
"""

import pytest
from pyspark.sql import functions as F

from calidad import motivos as M
from calidad.reglas import CARGOS, DEPARTAMENTOS, EMPLEADOS, evaluar, nombres_repetidos
from bronce import partir_en_lineas  # código real de ingesta (src/ingesta/bronce.py)
from fabricas import (
    INGESTA_1,
    INGESTA_2,
    LOTE_1,
    VALIDA,
    bronce,
    fila,
    ids,
    micros_utc,
    motivos_por_linea,
)


def evaluar_empleados(spark, filas, **kwargs):
    """Evalúa empleados con departamentos {1, 2, 3} y cargos {1, 2} existentes."""
    return evaluar(
        bronce(spark, EMPLEADOS, filas, **kwargs),
        EMPLEADOS,
        ids(spark, [1, 2, 3]),
        ids(spark, [1, 2]),
    )


def con_id(id_, base=VALIDA, **cambios):
    """Copia de una fila válida con otro id y algunos campos cambiados."""
    columnas = dict(zip(EMPLEADOS.columnas, base))
    columnas["id"] = id_
    columnas.update(cambios)
    return tuple(columnas[c] for c in EMPLEADOS.columnas)


# --- Fila válida ------------------------------------------------------------------------
def test_fila_valida_no_tiene_motivos_y_queda_tipada(spark):
    df = evaluar_empleados(spark, [VALIDA])
    r = df.select(
        "motivos", "_correcciones", "_t_id", "_t_id_departamento", "_t_id_cargo", "_t_nombre",
        F.unix_micros("_t_fecha_contratacion").alias("micros"),
    ).first()
    assert list(r["motivos"]) == []
    assert list(r["_correcciones"]) == []
    assert (r["_t_id"], r["_t_id_departamento"], r["_t_id_cargo"]) == (1, 1, 1)
    assert r["_t_nombre"] == "Ana Pérez"
    assert r["micros"] == micros_utc("2021-07-27T16:02:08Z")


# --- Corrección visible (D10) ------------------------------------------------------------
def test_recorte_de_espacios_es_una_correccion_registrada(spark):
    filas = [
        ("3", "  Talento Humano"),  # casos reales de departamentos.csv
        ("7", "Tecnología "),
        ("10", " Servicio al Cliente  "),
        ("4", "Ventas"),  # sin cambios
        ("5", "\tMercadeo\r"),  # tabulador y \r: trim() de Spark no los quita
        ("6", " Finanzas "),  # espacio duro (NBSP)
        (" 8 ", "Legal"),  # también en el id
    ]
    df = evaluar(bronce(spark, DEPARTAMENTOS, filas), DEPARTAMENTOS)
    resultado = {
        r["_t_id"]: (r["_t_departamento"], list(r["_correcciones"]), r["_valores_originales"]["departamento"])
        for r in df.collect()
    }
    corregido = [M.recorte_espacios("departamento")]
    assert resultado[3] == ("Talento Humano", corregido, "  Talento Humano")
    assert resultado[7] == ("Tecnología", corregido, "Tecnología ")
    assert resultado[10] == ("Servicio al Cliente", corregido, " Servicio al Cliente  ")
    assert resultado[4] == ("Ventas", [], "Ventas")
    assert resultado[5] == ("Mercadeo", corregido, "\tMercadeo\r")
    assert resultado[6] == ("Finanzas", corregido, " Finanzas ")
    assert resultado[8] == ("Legal", [M.recorte_espacios("id")], "Legal")
    assert all(list(r["motivos"]) == [] for r in df.collect())


# --- Regla 1: obligatorios (D1) ---------------------------------------------------------
@pytest.mark.parametrize("vacio", [None, "", "   ", "\t", " \r "])
def test_cualquier_forma_de_vacio_es_campo_vacio(spark, vacio):
    df = evaluar_empleados(spark, [con_id("1", nombre=vacio)])
    assert motivos_por_linea(df) == {1: [M.campo_vacio("nombre")]}


def test_cada_columna_vacia_tiene_su_motivo(spark):
    filas = [
        con_id(None),
        con_id("2", fecha_contratacion=""),
        con_id("3", id_departamento=None),
        con_id("4", id_cargo=""),
    ]
    assert motivos_por_linea(evaluar_empleados(spark, filas)) == {
        1: [M.campo_vacio("id")],
        2: [M.campo_vacio("fecha_contratacion")],
        3: [M.campo_vacio("id_departamento")],
        4: [M.campo_vacio("id_cargo")],
    }


# --- Regla 1: tipos de los enteros (D2) -------------------------------------------------
def test_enteros_invalidos_y_fuera_de_rango(spark):
    filas = [
        con_id("abc"),
        con_id("1.5"),
        con_id("+5"),
        con_id("0"),
        con_id("-3"),
        con_id("99999999999"),
        con_id("007"),  # vale 7
        con_id("8", id_departamento="x"),  # inválido: sin DEPARTAMENTO_NO_EXISTE además
        con_id("9", id_cargo="0"),
    ]
    df = evaluar_empleados(spark, filas)
    assert motivos_por_linea(df) == {
        1: [M.tipo_invalido("id")],
        2: [M.tipo_invalido("id")],
        3: [M.tipo_invalido("id")],
        4: [M.valor_fuera_de_rango("id")],
        5: [M.valor_fuera_de_rango("id")],
        6: [M.valor_fuera_de_rango("id")],
        7: [],
        8: [M.tipo_invalido("id_departamento")],
        9: [M.valor_fuera_de_rango("id_cargo")],
    }
    assert df.filter("_numero_linea = 7").first()["_t_id"] == 7


# --- Regla 1: fechas (D3) ---------------------------------------------------------------
@pytest.mark.parametrize(
    "fecha",
    [
        "27/07/2021 16:02",  # línea 238 del lote 1
        "20210727",  # línea 1097 del lote 1
        "ayer",  # línea 882 del lote 1
        "2021-07-27T16:02:08+00:00",
        "2021-07-27 16:02:08Z",
        "2021-07-27T16:02:08.123Z",
        "2021-07-27T16:02:08",
    ],
)
def test_fecha_con_otro_formato(spark, fecha):
    df = evaluar_empleados(spark, [con_id("1", fecha_contratacion=fecha)])
    assert motivos_por_linea(df) == {1: [M.FECHA_FORMATO_INVALIDO]}


@pytest.mark.parametrize(
    "fecha",
    [
        "2021-02-30T10:00:00Z",  # línea 325 del lote 1
        "2021-13-01T08:00:00Z",  # línea 787 del lote 1
        "2021-06-31T12:00:00Z",  # línea 1210 del lote 1
        "2021-02-29T00:00:00Z",  # 2021 no es bisiesto
        "2021-07-27T24:00:00Z",
    ],
)
def test_fecha_imposible_no_se_acomoda(spark, fecha):
    df = evaluar_empleados(spark, [con_id("1", fecha_contratacion=fecha)])
    r = df.first()
    assert list(r["motivos"]) == [M.FECHA_INEXISTENTE]
    assert r["_t_fecha_contratacion"] is None


def test_fecha_es_el_instante_utc_aunque_la_sesion_tenga_otra_zona(spark, sesion_bogota):
    # Bordes de año y trimestre en UTC; en Bogotá (UTC-5) caerían en otro año/trimestre.
    fechas = ["2021-01-01T00:00:00Z", "2020-12-31T23:59:59Z", "2021-04-01T01:19:11Z", "2024-02-29T12:00:00Z"]
    filas = [con_id(str(i), fecha_contratacion=f) for i, f in enumerate(fechas, start=1)]
    df = evaluar_empleados(spark, filas)
    obtenido = {
        r["_numero_linea"]: (list(r["motivos"]), r["micros"])
        for r in df.select("_numero_linea", "motivos", F.unix_micros("_t_fecha_contratacion").alias("micros")).collect()
    }
    assert obtenido == {i: ([], micros_utc(f)) for i, f in enumerate(fechas, start=1)}


# --- Regla 1: rango razonable (D4) ------------------------------------------------------
def test_fecha_fuera_de_rango(spark):
    # INGESTA_1 = 2026-11-01T06:00:00Z
    filas = [
        con_id("1", fecha_contratacion="2026-11-01T06:00:01Z"),  # después de su ingesta
        con_id("2", fecha_contratacion="1949-12-31T23:59:59Z"),  # antes del piso
        con_id("3", fecha_contratacion="1950-01-01T00:00:00Z"),  # justo en el piso
        con_id("4", fecha_contratacion="2026-11-01T06:00:00Z"),  # justo en su ingesta
    ]
    assert motivos_por_linea(evaluar_empleados(spark, filas, ingestado=INGESTA_1)) == {
        1: [M.FECHA_FUERA_DE_RANGO],
        2: [M.FECHA_FUERA_DE_RANGO],
        3: [],
        4: [],
    }


# --- Regla 2: referencias (D9) ----------------------------------------------------------
def test_departamento_y_cargo_que_no_existen(spark):
    filas = [
        con_id("1", id_departamento="15"),  # como las 5 filas del lote 1
        con_id("2", id_departamento="99", id_cargo="99"),
        con_id("3", id_cargo="7"),
        con_id("4", id_departamento=""),  # vacío: un solo motivo, no dos
    ]
    assert motivos_por_linea(evaluar_empleados(spark, filas)) == {
        1: [M.DEPARTAMENTO_NO_EXISTE],
        2: [M.DEPARTAMENTO_NO_EXISTE, M.CARGO_NO_EXISTE],
        3: [M.CARGO_NO_EXISTE],
        4: [M.campo_vacio("id_departamento")],
    }


# --- Varios motivos en una fila (D5) ----------------------------------------------------
def test_una_fila_con_varios_motivos_los_tiene_todos_en_orden_fijo(spark):
    filas = [("40", "", "ayer", "99", ""), ("41", " ", "2021-02-30T10:00:00Z", "15", "9")]
    assert motivos_por_linea(evaluar_empleados(spark, filas)) == {
        1: [
            M.campo_vacio("nombre"),
            M.FECHA_FORMATO_INVALIDO,
            M.campo_vacio("id_cargo"),
            M.DEPARTAMENTO_NO_EXISTE,
        ],
        2: [
            M.campo_vacio("nombre"),
            M.FECHA_INEXISTENTE,
            M.DEPARTAMENTO_NO_EXISTE,
            M.CARGO_NO_EXISTE,
        ],
    }


# --- Estructura de la línea (D14) -------------------------------------------------------
def test_lineas_vacias_o_con_columnas_de_mas_o_de_menos(spark):
    filas = [
        fila("50", "Ana Pérez", "2021-07-27T16:02:08Z", "1", "1",
             original="50,Ana Pérez,2021-07-27T16:02:08Z,1,1,sobra"),
        fila("51", "Ana Pérez", "2021-07-27T16:02:08Z", "1", None,
             original="51,Ana Pérez,2021-07-27T16:02:08Z,1"),
        fila(None, None, None, None, None, original=""),
        fila(None, None, None, None, None, original="   "),
    ]
    assert motivos_por_linea(evaluar_empleados(spark, filas)) == {
        1: [M.COLUMNAS_INVALIDAS],
        # Falta una columna, pero no se sabe cuál: no se adivina un CAMPO_VACIO:id_cargo.
        2: [M.COLUMNAS_INVALIDAS],
        3: [M.LINEA_VACIA],
        4: [M.LINEA_VACIA],
    }


# --- Comillas (D18): las líneas se parten con el código real de ingesta --------------------
def bronce_como_ingesta(spark, entidad, texto):
    """Filas bronce de un archivo con `texto`, partidas por `partir_en_lineas` de ingesta
    (split por comas, sin interpretar comillas)."""
    archivos = spark.createDataFrame([(texto, LOTE_1)], "value STRING, _archivo_origen STRING")
    archivos = archivos.withColumn("_archivo_modificado_en", F.current_timestamp())
    return partir_en_lineas(archivos, list(entidad.columnas))


def test_comillas_con_coma_adentro_dan_columnas_de_mas(spark):
    texto = '52,"Pérez, Ana",2021-07-27T16:02:08Z,1,1\r\n'
    filas = bronce_como_ingesta(spark, EMPLEADOS, texto)
    # Así la deja ingesta: la coma dentro de las comillas parte el nombre en dos campos.
    r = filas.first()
    assert (r["nombre"], r["fecha_contratacion"], r["id_departamento"]) == (
        '"Pérez', ' Ana"', "2021-07-27T16:02:08Z"
    )
    df = evaluar(filas, EMPLEADOS, ids(spark, [1, 2, 3]), ids(spark, [1, 2]))
    # Dos hechos de la línea; sin motivos por los campos corridos (fecha ' Ana"',
    # departamento '2021-07-27...'), que serían consecuencia de lo mismo.
    assert motivos_por_linea(df) == {1: [M.COLUMNAS_INVALIDAS, M.COMILLAS_NO_SOPORTADAS]}


def test_comillas_sin_coma_no_llegan_a_plata(spark):
    texto = (
        '53,"Ana Pérez",2021-07-27T16:02:08Z,1,1\r\n'  # sin la regla, iría a plata con comillas
        '"54",Luis Gómez,2021-07-27T16:02:08Z,1,1\r\n'  # id con comillas: sin TIPO_INVALIDO:id
        '55,"Sara Vélez",,1,99\r\n'  # las otras causas sí se reportan
        '56,Eva Mora,2021-07-27T16:02:08Z,1,1\r\n'  # control: válida
    )
    df = evaluar(
        bronce_como_ingesta(spark, EMPLEADOS, texto), EMPLEADOS, ids(spark, [1, 2, 3]), ids(spark, [1, 2])
    )
    assert motivos_por_linea(df) == {
        1: [M.COMILLAS_NO_SOPORTADAS],
        2: [M.COMILLAS_NO_SOPORTADAS],
        3: [M.COMILLAS_NO_SOPORTADAS, M.campo_vacio("fecha_contratacion"), M.CARGO_NO_EXISTE],
        4: [],
    }
    # También en las dimensiones.
    cargos = evaluar(bronce_como_ingesta(spark, CARGOS, '7,"Abogado"\r\n8,Auditor\r\n'), CARGOS)
    assert motivos_por_linea(cargos) == {1: [M.COMILLAS_NO_SOPORTADAS], 2: []}


# --- Regla 3: unicidad dentro de un lote (D6, D7) ---------------------------------------
def test_copias_exactas_conservan_la_primera_y_conflictos_van_todos(spark):
    filas = [
        con_id("7"),  # 1: se conserva
        con_id("8", fecha_contratacion="2020-03-23T03:31:10Z", id_departamento="1"),  # 2: conflicto
        con_id("8", fecha_contratacion="2020-08-26T14:30:28Z", id_departamento="2"),  # 3: conflicto
        con_id("9", nombre="Luis Gómez"),  # 4: se conserva
        con_id("7"),  # 5: copia
        con_id("9", nombre="  Luis Gómez "),  # 6: copia (igual tras recortar)
        con_id("010"),  # 7: id 10, se conserva
        con_id("7"),  # 8: copia
        con_id("10"),  # 9: copia de la 7 (010 y 10 son el mismo entero)
        con_id("11"),  # 10: conflicto con la 11 (le falta el nombre)
        con_id("11", nombre=""),  # 11: conflicto y además campo vacío
    ]
    assert motivos_por_linea(evaluar_empleados(spark, filas)) == {
        1: [],
        2: [M.ID_DUPLICADO_CONFLICTO],
        3: [M.ID_DUPLICADO_CONFLICTO],
        4: [],
        5: [M.ID_DUPLICADO_COPIA],
        6: [M.ID_DUPLICADO_COPIA],
        7: [],
        8: [M.ID_DUPLICADO_COPIA],
        9: [M.ID_DUPLICADO_COPIA],
        10: [M.ID_DUPLICADO_CONFLICTO],
        11: [M.campo_vacio("nombre"), M.ID_DUPLICADO_CONFLICTO],
    }


def test_la_copia_que_se_conserva_es_la_primera_ingestada(spark):
    # Mismo contenido en dos archivos: gana el ingestado primero, no el de menor línea.
    temprano = bronce(spark, EMPLEADOS, [fila(*VALIDA, linea=9)], archivo="b.csv", ingestado=INGESTA_1)
    tarde = bronce(spark, EMPLEADOS, [fila(*VALIDA, linea=1)], archivo="a.csv", ingestado=INGESTA_2)
    df = evaluar(temprano.unionByName(tarde), EMPLEADOS, ids(spark, [1]), ids(spark, [1]))
    obtenido = {r["_archivo_origen"]: list(r["motivos"]) for r in df.collect()}
    assert obtenido == {"b.csv": [], "a.csv": [M.ID_DUPLICADO_COPIA]}


# --- Dimensiones (D12) ------------------------------------------------------------------
def test_reglas_en_dimensiones(spark):
    filas = [("1", "Ventas"), ("1", "Compras"), ("2", ""), ("3", "Legal"), ("3", " Legal ")]
    assert motivos_por_linea(evaluar(bronce(spark, CARGOS, filas), CARGOS)) == {
        1: [M.ID_DUPLICADO_CONFLICTO],
        2: [M.ID_DUPLICADO_CONFLICTO],
        3: [M.campo_vacio("cargo")],
        4: [],
        5: [M.ID_DUPLICADO_COPIA],
    }


def test_nombres_repetidos_en_una_dimension_solo_se_reportan(spark):
    plata = spark.createDataFrame(
        [(7, "Tecnología"), (13, "tecnologia"), (4, "Ventas")], "id INT, departamento STRING"
    )
    repetidos = nombres_repetidos(plata, "departamento").collect()
    assert [(r["nombre_normalizado"], list(r["ids"])) for r in repetidos] == [("tecnologia", [7, 13])]
