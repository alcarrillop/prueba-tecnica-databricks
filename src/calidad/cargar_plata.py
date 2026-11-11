# Databricks notebook source
# MAGIC %md
# MAGIC # Calidad: bronce -> plata y cuarentena
# MAGIC
# MAGIC Aplica las reglas de la Sección 2 a las filas nuevas de `bronce_departamentos`,
# MAGIC `bronce_cargos` y `bronce_empleados`. Las válidas van a `plata_<entidad>` y las demás
# MAGIC a `cuarentena` con todos sus motivos. La lógica vive en `src/calidad/` (módulos
# MAGIC importables y probados); este notebook solo recibe parámetros y la invoca.
# MAGIC
# MAGIC Falla si alguna fila bronce queda sin destino o en las dos tablas a la vez.

# COMMAND ----------

dbutils.widgets.text("catalogo", "")
dbutils.widgets.text("esquema", "")
catalogo = dbutils.widgets.get("catalogo").strip()
esquema = dbutils.widgets.get("esquema").strip()
if not catalogo or not esquema:
    raise ValueError("Faltan los parámetros `catalogo` y `esquema` (los pasa el job).")

# COMMAND ----------

import os
import sys

# El directorio del notebook es src/calidad/; el paquete `calidad` se importa desde src/.
sys.path.insert(0, os.path.dirname(os.getcwd()))

from calidad.incremental import (  # noqa: E402
    Tablas,
    avisos_dimensiones,
    conciliar,
    motivos_por_entidad,
    procesar,
)

tablas = Tablas(catalogo, esquema)

# COMMAND ----------

escrituras = procesar(spark, tablas)
for fila in escrituras:
    print(fila)

# COMMAND ----------

# Ninguna fila se pierde: bronce = plata + cuarentena, sin filas en las dos.
conciliacion = conciliar(spark, tablas)
for fila in conciliacion:
    print(fila)
problemas = [f for f in conciliacion if f["sin_destino"] or f["en_ambas"] or f["huerfanas"]]
if problemas:
    raise AssertionError(f"La conciliación de calidad falló: {problemas}")

# COMMAND ----------

display(motivos_por_entidad(spark, tablas))

# COMMAND ----------

# D12: nombres repetidos con ids distintos en las dimensiones (solo aviso).
avisos = {}
for entidad, df in avisos_dimensiones(spark, tablas).items():
    avisos[entidad] = [fila.asDict() for fila in df.collect()]
    print(f"{entidad}: {len(avisos[entidad])} nombres repetidos con ids distintos")
    for fila in avisos[entidad]:
        print("  ", fila)

# COMMAND ----------

import json  # noqa: E402

# Resumen visible en la salida de la tarea del job (escrituras de esta corrida y conciliación).
dbutils.notebook.exit(
    json.dumps(
        {"escrituras": escrituras, "conciliacion": conciliacion, "nombres_repetidos": avisos},
        ensure_ascii=False,
    )
)
