"""Reglas de calidad de la Sección 2, como funciones puras sobre DataFrames.

Nada de este módulo lee ni escribe tablas: recibe DataFrames con la forma de las tablas
bronce (todas las columnas de datos en STRING, más los metadatos de ingesta) y devuelve
DataFrames. Así las pruebas ejercitan exactamente este código con datos hechos a mano.

Flujo de `evaluar` (cada paso es una función con nombre propio):
  1. `guardar_valores_originales`: copia de los valores tal como llegaron (para cuarentena).
  2. `recortar_espacios`: CORRECCIÓN visible (D10), registrada en `_correcciones`.
  3. `validar_estructura`: línea vacía, con columnas de más o de menos (contadas por
     comas, igual que ingesta) o con comillas dobles (D14, D18).
  4. `validar_campos`: obligatorios y tipos (D1-D4); deja los valores tipados en `_t_<col>`.
     No evalúa campos corridos ni campos con comillas (un motivo por causa).
  5. `validar_referencias`: departamento y cargo existen (solo empleados; D9).
  6. `marcar_duplicados`: unicidad del id dentro del conjunto evaluado (D6-D8).
  7. `combinar_motivos`: arreglo `motivos` con todos los motivos de la fila, en orden fijo.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from calidad import motivos as M

# --- Metadatos que agrega ingesta a cada tabla bronce ----------------------------------
ARCHIVO = "_archivo_origen"
LINEA = "_numero_linea"
LINEA_ORIGINAL = "_linea_original"
INGESTADO = "_ingestado_en"
CLAVE = [ARCHIVO, LINEA]  # identifica una fila cruda

# --- Definiciones (D1-D4) ---------------------------------------------------------------
# Espacios en blanco de borde: espacio, tabulador, \r, \n, \f, \v, espacio duro (NBSP) y
# BOM. `trim()` de Spark solo quita el espacio 0x20, por eso se usa una regex explícita.
ESPACIOS_BORDE = r"^[\s ﻿]+|[\s ﻿]+$"
PATRON_ENTERO = r"^-?[0-9]+$"
ENTERO_MAXIMO = 2_147_483_647  # los ids se guardan como INT
# Única forma aceptada (D3): la del enunciado, UTC con Z. `X` interpreta la Z como UTC,
# así que el instante no depende de la zona horaria de la sesión.
PATRON_FECHA = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
FORMATO_FECHA = "yyyy-MM-dd'T'HH:mm:ssX"
FECHA_MINIMA = "1950-01-01T00:00:00Z"  # piso fijo de D4
# Las columnas se cuentan igual que las parte ingesta: por cada coma, sin interpretar
# comillas (`split(linea, ',', -1)`). Las comillas tienen su propio motivo (D18).
SEPARADOR = ","
COMILLA = '"'


@dataclass(frozen=True)
class Entidad:
    """Una de las tres entidades del enunciado, con sus columnas en el orden del archivo."""

    nombre: str
    columnas: tuple[str, ...]
    enteras: tuple[str, ...]
    fecha: str | None = None

    def tipo_de(self, columna: str) -> str:
        if columna in self.enteras:
            return "entero"
        if columna == self.fecha:
            return "fecha"
        return "texto"


DEPARTAMENTOS = Entidad("departamentos", ("id", "departamento"), enteras=("id",))
CARGOS = Entidad("cargos", ("id", "cargo"), enteras=("id",))
EMPLEADOS = Entidad(
    "empleados",
    ("id", "nombre", "fecha_contratacion", "id_departamento", "id_cargo"),
    enteras=("id", "id_departamento", "id_cargo"),
    fecha="fecha_contratacion",
)
ENTIDADES = {e.nombre: e for e in (DEPARTAMENTOS, CARGOS, EMPLEADOS)}


def _t(columna: str) -> str:
    """Nombre de la columna interna con el valor ya tipado."""
    return f"_t_{columna}"


def _compactar(*columnas: Column) -> Column:
    """Arreglo con los valores no nulos, en el orden dado (los motivos que aplican)."""
    return F.filter(F.array(*columnas), lambda x: x.isNotNull())


def _arreglo_vacio() -> Column:
    return F.array().cast(T.ArrayType(T.StringType()))


def quitar_espacios_borde(columna: Column) -> Column:
    return F.regexp_replace(columna, ESPACIOS_BORDE, "")


def es_vacio(columna: Column) -> Column:
    """D1: NULL, cadena vacía o solo espacios en blanco."""
    return columna.isNull() | (quitar_espacios_borde(columna) == "")


def id_normalizado(columna: Column) -> Column:
    """El id como entero válido (D2), o NULL si no lo es. Misma lógica que `validar_campos`."""
    v = quitar_espacios_borde(columna)
    numero = v.try_cast("decimal(38,0)")
    return F.when(
        v.rlike(PATRON_ENTERO) & numero.between(1, ENTERO_MAXIMO), numero.cast("int")
    )


# --- Paso 1 -----------------------------------------------------------------------------
def guardar_valores_originales(df: DataFrame, entidad: Entidad) -> DataFrame:
    """Copia los valores de datos tal como llegaron, antes de cualquier corrección."""
    pares = [x for c in entidad.columnas for x in (F.lit(c), F.col(c))]
    return df.withColumn("_valores_originales", F.create_map(*pares))


# --- Paso 2: CORRECCIÓN -------------------------------------------------------------------
def recortar_espacios(df: DataFrame, columnas: tuple[str, ...] | list[str]) -> DataFrame:
    """CORRECCIÓN (D10): quita espacios en blanco de los bordes de cada columna de datos.

    No adivina nada: `"  Talento Humano"` y `"Talento Humano"` son el mismo valor. Cada
    columna tocada queda registrada en `_correcciones` como `RECORTE_ESPACIOS:<columna>`
    (también en las columnas enteras y de fecha: `" 12 "` pasa a `"12"`). El valor
    original sigue en la tabla bronce y en `_valores_originales`.
    """
    marcas = []
    for c in columnas:
        recortado = quitar_espacios_borde(F.col(c))
        marcas.append(F.when(F.col(c) != recortado, F.lit(M.recorte_espacios(c))))
    df = df.withColumn("_correcciones", _compactar(*marcas))
    for c in columnas:
        df = df.withColumn(c, quitar_espacios_borde(F.col(c)))
    return df


# --- Paso 3 -----------------------------------------------------------------------------
def validar_estructura(df: DataFrame, entidad: Entidad) -> DataFrame:
    """D14 y D18: estructura de la línea cruda (`_linea_original`, que guarda ingesta).

    - `LINEA_VACIA`: la línea no tiene nada.
    - `COLUMNAS_INVALIDAS`: partida por comas como lo hace ingesta (sin interpretar
      comillas), no da el número de columnas del enunciado. Al partir, una columna de más
      se descarta y una de menos queda vacía sin dejar rastro en los campos.
    - `COMILLAS_NO_SOPORTADAS`: la línea (o algún campo) trae una comilla doble.

    Deja `_columnas_invalidas` para que los pasos siguientes no evalúen campos corridos.
    """
    linea = F.col(LINEA_ORIGINAL)
    vacia = linea.isNotNull() & (quitar_espacios_borde(linea) == "")
    n_columnas = F.size(F.split(linea, SEPARADOR, -1))
    columnas_invalidas = linea.isNotNull() & ~vacia & (n_columnas != len(entidad.columnas))
    comillas = F.coalesce(linea.contains(COMILLA), F.lit(False))
    for c in entidad.columnas:
        comillas = comillas | F.coalesce(F.col(c).contains(COMILLA), F.lit(False))
    return (
        df.withColumn("_linea_vacia", F.coalesce(vacia, F.lit(False)))
        .withColumn("_columnas_invalidas", F.coalesce(columnas_invalidas, F.lit(False)))
        .withColumn(
            "_m_estructura",
            _compactar(
                F.when(F.col("_columnas_invalidas"), F.lit(M.COLUMNAS_INVALIDAS)),
                F.when(comillas, F.lit(M.COMILLAS_NO_SOPORTADAS)),
            ),
        )
    )


def _campo_evaluable(columna: str) -> Column:
    """Un motivo por causa (D5, D18): no se evalúa un campo cuyo error sería consecuencia
    de la estructura. Con columnas de más o de menos los campos están corridos y no se
    sabe qué columna falta o sobra; un campo con comillas ya tiene su motivo."""
    return ~F.col("_columnas_invalidas") & ~F.coalesce(
        F.col(columna).contains(COMILLA), F.lit(False)
    )


# --- Paso 4 -----------------------------------------------------------------------------
def validar_campos(df: DataFrame, entidad: Entidad) -> DataFrame:
    """Regla 1 (D1-D4) sobre los valores ya recortados. Deja los tipados en `_t_<col>`.

    - Vacío (D1): NULL o `''` -> `CAMPO_VACIO:<col>` (los espacios ya se quitaron).
    - Entero (D2): `-?[0-9]+`; si no -> `TIPO_INVALIDO:<col>`. Con forma de entero pero
      fuera de 1..2147483647 -> `VALOR_FUERA_DE_RANGO:<col>`. `007` vale 7.
    - Fecha (D3): solo `AAAA-MM-DDTHH:MM:SSZ`; otra forma -> `FECHA_FORMATO_INVALIDO`;
      con la forma pero imposible en el calendario -> `FECHA_INEXISTENTE` (se parsea
      estricto, sin "acomodar" 2021-02-30 a marzo).
    - Rango (D4): posterior a su `_ingestado_en` o anterior a 1950 -> `FECHA_FUERA_DE_RANGO`.

    Los valores se tipan siempre (el id tipado se usa para la unicidad), pero los motivos
    de un campo solo se agregan si el campo es evaluable (`_campo_evaluable`, D18).
    """
    motivos_por_columna = []
    for c in entidad.columnas:
        v = F.col(c)
        vacio = v.isNull() | (v == "")
        tipo = entidad.tipo_de(c)
        motivos_columna = [F.when(vacio, F.lit(M.campo_vacio(c)))]

        if tipo == "entero":
            forma = v.rlike(PATRON_ENTERO)
            numero = v.try_cast("decimal(38,0)")
            en_rango = F.coalesce(numero.between(1, ENTERO_MAXIMO), F.lit(False))
            df = df.withColumn(_t(c), F.when(~vacio & forma & en_rango, numero.cast("int")))
            motivos_columna.append(F.when(~vacio & ~forma, F.lit(M.tipo_invalido(c))))
            motivos_columna.append(
                F.when(~vacio & forma & ~en_rango, F.lit(M.valor_fuera_de_rango(c)))
            )
        elif tipo == "fecha":
            forma = v.rlike(PATRON_FECHA)
            df = df.withColumn(
                _t(c), F.when(~vacio & forma, F.try_to_timestamp(v, F.lit(FORMATO_FECHA)))
            )
            ts = F.col(_t(c))
            minima = F.try_to_timestamp(F.lit(FECHA_MINIMA), F.lit(FORMATO_FECHA))
            fuera = ts.isNotNull() & ((ts < minima) | (ts > F.col(INGESTADO)))
            motivos_columna.append(F.when(~vacio & ~forma, F.lit(M.FECHA_FORMATO_INVALIDO)))
            motivos_columna.append(
                F.when(~vacio & forma & ts.isNull(), F.lit(M.FECHA_INEXISTENTE))
            )
            motivos_columna.append(F.when(fuera, F.lit(M.FECHA_FUERA_DE_RANGO)))
        else:
            df = df.withColumn(_t(c), F.when(~vacio, v))

        evaluable = _campo_evaluable(c)
        motivos_por_columna.extend(F.when(evaluable, m) for m in motivos_columna)

    return df.withColumn("_m_campos", _compactar(*motivos_por_columna))


# --- Paso 5 -----------------------------------------------------------------------------
def validar_referencias(
    df: DataFrame, ids_departamentos: DataFrame, ids_cargos: DataFrame
) -> DataFrame:
    """Regla 2 (D9): el departamento y el cargo existen en las tablas plata.

    `ids_departamentos` e `ids_cargos` tienen una columna `id` (INT). Solo se evalúa
    cuando el valor es un entero válido: un campo vacío o inválido ya tiene su motivo y no
    suma un segundo por la misma causa (D5).
    """
    deps = ids_departamentos.select(F.col("id").alias("_ref_departamento")).distinct()
    cargos = ids_cargos.select(F.col("id").alias("_ref_cargo")).distinct()
    df = df.join(deps, F.col(_t("id_departamento")) == F.col("_ref_departamento"), "left")
    df = df.join(cargos, F.col(_t("id_cargo")) == F.col("_ref_cargo"), "left")
    # Con columnas corridas (D18) los valores no son confiables: no se busca la referencia.
    alineada = ~F.col("_columnas_invalidas")
    falta_dep = (
        alineada & F.col(_t("id_departamento")).isNotNull() & F.col("_ref_departamento").isNull()
    )
    falta_cargo = alineada & F.col(_t("id_cargo")).isNotNull() & F.col("_ref_cargo").isNull()
    return df.withColumn(
        "_m_referencias",
        _compactar(
            F.when(falta_dep, F.lit(M.DEPARTAMENTO_NO_EXISTE)),
            F.when(falta_cargo, F.lit(M.CARGO_NO_EXISTE)),
        ),
    ).drop("_ref_departamento", "_ref_cargo")


# --- Paso 6 -----------------------------------------------------------------------------
def huella(entidad: Entidad) -> Column:
    """Valores normalizados de la fila, para decidir si dos filas son copia exacta (D6).

    Se compara después de recortar y tipar: `" Ana"` y `"Ana"`, o `007` y `7`, son el
    mismo valor. La fecha se compara como instante (microsegundos desde 1970 en UTC),
    sin depender de la zona horaria de la sesión. Un valor que no se pudo tipar se
    compara como texto recortado.
    """
    campos = []
    for c in entidad.columnas:
        tipo = entidad.tipo_de(c)
        if tipo == "entero":
            valor = F.coalesce(F.col(_t(c)).cast("string"), F.col(c))
        elif tipo == "fecha":
            valor = F.coalesce(F.unix_micros(F.col(_t(c))).cast("string"), F.col(c))
        else:
            valor = F.col(c)
        campos.append(valor.alias(c))
    return F.to_json(F.struct(*campos))


def marcar_duplicados(df: DataFrame, entidad: Entidad) -> DataFrame:
    """Regla 3 (D5-D8): el id identifica a un único registro.

    Se agrupa por el id tipado. Participan también las filas que fallan por otro motivo
    (D5). Dentro de un grupo de más de una fila:
      - si todas son copia exacta, se conserva la primera por (`_ingestado_en`, archivo,
        línea) y las demás reciben `ID_DUPLICADO_COPIA`;
      - si hay datos distintos, todas reciben `ID_DUPLICADO_CONFLICTO` (no se adivina
        cuál es la verdadera).
    La historia la arma `incremental.planificar`: el DataFrame que llega aquí ya incluye
    todas las filas anteriores que comparten id con las nuevas.
    """
    grupo = Window.partitionBy(_t("id"))
    orden = grupo.orderBy(F.col(INGESTADO).asc_nulls_last(), F.col(ARCHIVO), F.col(LINEA))
    df = (
        df.withColumn("_huella", huella(entidad))
        .withColumn("_n_filas_id", F.count(F.lit(1)).over(grupo))
        .withColumn("_n_versiones_id", F.size(F.collect_set("_huella").over(grupo)))
        .withColumn("_orden_id", F.row_number().over(orden))
    )
    con_id = F.col(_t("id")).isNotNull() & (F.col("_n_filas_id") > 1)
    copia = con_id & (F.col("_n_versiones_id") == 1) & (F.col("_orden_id") > 1)
    conflicto = con_id & (F.col("_n_versiones_id") > 1)
    return df.withColumn(
        "_m_duplicados",
        _compactar(
            F.when(copia, F.lit(M.ID_DUPLICADO_COPIA)),
            F.when(conflicto, F.lit(M.ID_DUPLICADO_CONFLICTO)),
        ),
    ).drop("_huella", "_n_filas_id", "_n_versiones_id", "_orden_id")


# --- Paso 7 -----------------------------------------------------------------------------
def combinar_motivos(df: DataFrame) -> DataFrame:
    """Todos los motivos de la fila, en orden fijo: estructura, campos (en el orden de las
    columnas), referencias y unicidad. Un motivo por causa:
      - una línea vacía lleva solo `LINEA_VACIA`;
      - con `COLUMNAS_INVALIDAS` no hay motivos de campos ni de referencias (están corridos);
      - un campo con comillas no suma motivos propios: ya cuenta `COMILLAS_NO_SOPORTADAS`.
    `COLUMNAS_INVALIDAS` y `COMILLAS_NO_SOPORTADAS` pueden ir juntos: son dos hechos
    distintos de la línea (no tiene 5 campos; trae comillas)."""
    referencias = F.col("_m_referencias") if "_m_referencias" in df.columns else _arreglo_vacio()
    todos = F.concat(
        F.col("_m_estructura"), F.col("_m_campos"), referencias, F.col("_m_duplicados")
    )
    return df.withColumn(
        "motivos", F.when(F.col("_linea_vacia"), F.array(F.lit(M.LINEA_VACIA))).otherwise(todos)
    )


def evaluar(
    bronce: DataFrame,
    entidad: Entidad,
    ids_departamentos: DataFrame | None = None,
    ids_cargos: DataFrame | None = None,
) -> DataFrame:
    """Aplica todas las reglas. Devuelve las mismas filas con `_t_<col>` (valores
    tipados), `_valores_originales`, `_correcciones` y `motivos` (vacío = fila válida)."""
    df = guardar_valores_originales(bronce, entidad)
    df = recortar_espacios(df, entidad.columnas)
    df = validar_estructura(df, entidad)
    df = validar_campos(df, entidad)
    if entidad is EMPLEADOS:
        if ids_departamentos is None or ids_cargos is None:
            raise ValueError("Empleados necesita los ids de departamentos y cargos plata.")
        df = validar_referencias(df, ids_departamentos, ids_cargos)
    df = marcar_duplicados(df, entidad)
    df = combinar_motivos(df)
    return df.drop(
        "_m_estructura", "_m_campos", "_m_referencias", "_m_duplicados", "_linea_vacia",
        "_columnas_invalidas",
    )


# --- Proyección a las tablas de destino --------------------------------------------------
def esquema_plata(entidad: Entidad) -> T.StructType:
    tipos = {"entero": T.IntegerType(), "fecha": T.TimestampType(), "texto": T.StringType()}
    datos = [T.StructField(c, tipos[entidad.tipo_de(c)], False) for c in entidad.columnas]
    return T.StructType(
        datos
        + [
            T.StructField(ARCHIVO, T.StringType(), False),
            T.StructField(LINEA, T.IntegerType(), False),
            T.StructField("_correcciones", T.ArrayType(T.StringType()), False),
            T.StructField(INGESTADO, T.TimestampType(), True),
            T.StructField("_procesado_en", T.TimestampType(), False),
        ]
    )


ESQUEMA_CUARENTENA = T.StructType(
    [
        T.StructField("entidad", T.StringType(), False),
        T.StructField(ARCHIVO, T.StringType(), False),
        T.StructField(LINEA, T.IntegerType(), False),
        T.StructField(LINEA_ORIGINAL, T.StringType(), True),
        T.StructField("id_original", T.StringType(), True),
        T.StructField("valores_originales", T.MapType(T.StringType(), T.StringType()), False),
        T.StructField("motivos", T.ArrayType(T.StringType()), False),
        T.StructField(INGESTADO, T.TimestampType(), True),
        T.StructField("rechazado_en", T.TimestampType(), False),
    ]
)


def a_plata(evaluadas: DataFrame, entidad: Entidad) -> DataFrame:
    """Filas válidas con la forma de `plata_<entidad>` (valores tipados y recortados)."""
    datos = [F.col(_t(c)).alias(c) for c in entidad.columnas]
    return evaluadas.select(
        *datos,
        F.col(ARCHIVO),
        F.col(LINEA).cast("int").alias(LINEA),
        F.col("_correcciones"),
        F.col(INGESTADO),
        F.current_timestamp().alias("_procesado_en"),
    )


def a_cuarentena(evaluadas: DataFrame, entidad: Entidad) -> DataFrame:
    """Filas rechazadas con la forma de `cuarentena`: la fila tal como llegó y sus motivos."""
    return evaluadas.select(
        F.lit(entidad.nombre).alias("entidad"),
        F.col(ARCHIVO),
        F.col(LINEA).cast("int").alias(LINEA),
        F.col(LINEA_ORIGINAL),
        F.col("_valores_originales").getItem("id").alias("id_original"),
        F.col("_valores_originales").alias("valores_originales"),
        F.col("motivos"),
        F.col(INGESTADO),
        F.current_timestamp().alias("rechazado_en"),
    )


def nombres_repetidos(plata_dimension: DataFrame, columna: str) -> DataFrame:
    """D12: mismo nombre (sin distinguir mayúsculas ni tildes) con ids distintos en una
    dimensión. Solo se reporta; no manda nada a cuarentena."""
    clave = F.translate(F.lower(F.col(columna)), "áéíóúüñ", "aeiouun")
    return (
        plata_dimension.groupBy(clave.alias("nombre_normalizado"))
        .agg(F.sort_array(F.collect_set("id")).alias("ids"), F.collect_set(columna).alias("nombres"))
        .filter(F.size("ids") > 1)
    )
