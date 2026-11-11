# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta a bronce
# MAGIC
# MAGIC Carga incremental de los CSV del Volume `landing` a las tablas crudas
# MAGIC `bronce_departamentos`, `bronce_cargos` y `bronce_empleados`.
# MAGIC
# MAGIC - Solo procesa archivos que el checkpoint aún no ha visto (Auto Loader, `availableNow`).
# MAGIC - Repetirla sin archivos nuevos no escribe nada.
# MAGIC - La lógica está en `bronce.py` (mismo directorio); este notebook solo la invoca.
# MAGIC
# MAGIC Parámetros (los pasa el job): `catalogo`, `esquema`.

# COMMAND ----------

import json
import os
import sys

# El directorio del notebook es el directorio de trabajo; lo agregamos al path para
# importar el módulo vecino.
if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())

from bronce import cargar_todo  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalogo", "")
dbutils.widgets.text("esquema", "")
catalogo = dbutils.widgets.get("catalogo").strip()
esquema = dbutils.widgets.get("esquema").strip()
if not catalogo or not esquema:
    raise ValueError("Faltan los parámetros `catalogo` y `esquema` (los pasa el job).")

# COMMAND ----------

resumen = cargar_todo(spark, catalogo, esquema)
for fila in resumen:
    print(fila)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(resumen))
