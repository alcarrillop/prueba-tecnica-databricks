"""Códigos estables y legibles de rechazo (cuarentena) y de corrección (plata).

Los códigos no cambian entre corridas: las pruebas, el README y cualquier consulta sobre
la cuarentena dependen de ellos. Los que llevan columna usan el formato `CODIGO:columna`.
"""

# --- Estructura de la línea cruda (D14) ------------------------------------------------
LINEA_VACIA = "LINEA_VACIA"  # la línea no tiene nada (o solo espacios)
COLUMNAS_INVALIDAS = "COLUMNAS_INVALIDAS"  # partida por comas, no da el número de columnas esperado
# Algún campo trae una comilla doble. El formato del enunciado no usa comillas e ingesta
# parte por comas sin interpretarlas: un valor entrecomillado no es confiable.
COMILLAS_NO_SOPORTADAS = "COMILLAS_NO_SOPORTADAS"

# --- Regla 1: todos los campos son obligatorios (D1, D2, D3, D4) -----------------------
FECHA_FORMATO_INVALIDO = "FECHA_FORMATO_INVALIDO"  # no es AAAA-MM-DDTHH:MM:SSZ
FECHA_INEXISTENTE = "FECHA_INEXISTENTE"  # tiene la forma ISO pero no existe (2021-02-30)
FECHA_FUERA_DE_RANGO = "FECHA_FUERA_DE_RANGO"  # posterior a su ingesta o anterior a 1950


def campo_vacio(columna: str) -> str:
    """NULL, cadena vacía o solo espacios en blanco."""
    return f"CAMPO_VACIO:{columna}"


def tipo_invalido(columna: str) -> str:
    """Un entero que no es entero (`abc`, `1.5`, `+5`)."""
    return f"TIPO_INVALIDO:{columna}"


def valor_fuera_de_rango(columna: str) -> str:
    """Un entero que no es un id válido: 0, negativo o mayor que INT."""
    return f"VALOR_FUERA_DE_RANGO:{columna}"


# --- Regla 2: departamento y cargo existen (D9, D11) -----------------------------------
DEPARTAMENTO_NO_EXISTE = "DEPARTAMENTO_NO_EXISTE"
CARGO_NO_EXISTE = "CARGO_NO_EXISTE"

# --- Regla 3: el id identifica a un único registro (D6, D7, D8) ------------------------
ID_DUPLICADO_COPIA = "ID_DUPLICADO_COPIA"  # copia exacta de otra fila que ya se conserva
ID_DUPLICADO_CONFLICTO = "ID_DUPLICADO_CONFLICTO"  # mismo id con datos distintos


# --- Correcciones aplicadas a filas que sí pasan (columna `_correcciones`) -------------
def recorte_espacios(columna: str) -> str:
    """Se quitaron espacios en blanco de los bordes del valor (D10)."""
    return f"RECORTE_ESPACIOS:{columna}"
