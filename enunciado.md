# Prueba técnica: Ingeniero de Datos (Databricks)

**Tiempo sugerido:** 3 días · **Entrega:** un repositorio con el código y un README.

## Contexto
La empresa está migrando su sistema de talento humano a Databricks. El sistema viejo exporta tres archivos CSV: departamentos, cargos y empleados contratados. Los empleados llegan en **dos entregas**: primero el lote 1 y, días después, el lote 2.

Tu trabajo es construir el pipeline que recibe esos archivos, los deja en tablas confiables y responde dos preguntas del área de talento humano.

## Los datos
Están en `datos/`. Los archivos **no tienen encabezado** y vienen separados por comas.

**`departamentos.csv`**
| Columna | Tipo | Descripción |
|---|---|---|
| id | entero | Id del departamento |
| departamento | texto | Nombre del departamento |

**`cargos.csv`**
| Columna | Tipo | Descripción |
|---|---|---|
| id | entero | Id del cargo |
| cargo | texto | Nombre del cargo |

**`empleados_contratados_lote1.csv`** y **`empleados_contratados_lote2.csv`**
| Columna | Tipo | Descripción |
|---|---|---|
| id | entero | Id del empleado |
| nombre | texto | Nombre y apellido |
| fecha_contratacion | texto | Fecha y hora de contratación en formato ISO, en UTC (`2021-07-27T16:02:08Z`) |
| id_departamento | entero | Departamento para el que fue contratado |
| id_cargo | entero | Cargo para el que fue contratado |

## Sección 1: Ingesta
1. Sube los CSV a un Volume de Unity Catalog y cárgalos a tablas Delta.
2. La carga debe ser **incremental**: primero se procesa el lote 1; cuando llega el lote 2, solo se procesa lo nuevo.
3. La carga debe poder **repetirse sin dañar nada**: correr el pipeline dos veces con los mismos archivos deja las tablas exactamente igual.

## Sección 2: Calidad
Reglas del negocio:
- Todos los campos son obligatorios.
- Cada empleado debe pertenecer a un departamento y a un cargo que existan.
- El id identifica a un único empleado.

Las filas que no cumplan las reglas **no entran a las tablas finales, pero tampoco se pierden**: van a una tabla de cuarentena con el motivo del rechazo.

Los datos vienen de un sistema viejo. Reporta en el README cualquier otro problema que encuentres y qué decidiste hacer con él.

## Sección 3: SQL
Talento humano necesita dos tablas listas para consultar:

**Métrica 1.** Número de empleados contratados en 2021 por departamento y cargo, dividido por trimestre. Ordenada alfabéticamente por departamento y cargo.

| departamento | cargo | Q1 | Q2 | Q3 | Q4 |
|---|---|---|---|---|---|
| Finanzas | Contador | 3 | 0 | 7 | 11 |
| Finanzas | Gerente | 2 | 1 | 0 | 2 |
| Ventas | Gerente | 0 | 1 | 3 | 0 |

**Métrica 2.** Id, nombre y número de contratados de cada departamento que contrató más empleados que el promedio de todos los departamentos en 2021. Ordenada por número de contratados, de mayor a menor.

| id | departamento | contratados |
|---|---|---|
| 7 | Finanzas | 45 |
| 9 | Ventas | 12 |

## Bono
- Despliega todo con un bundle de Databricks, con ambientes de desarrollo y producción.
- Agrega pruebas automáticas (de las reglas de calidad y de las dos métricas).
- Deja el pipeline como un job programado.

## Qué entregar
- El código en un repositorio, con commits frecuentes que dejen ver el proceso.
- Un `README.md` con: cómo correrlo, la arquitectura, **las decisiones que tomaste y por qué**, y los problemas que encontraste en los datos.

## Rúbrica (100 puntos)
| Criterio | Puntos |
|---|---|
| Ingesta: las tres tablas cargan bien y la carga es incremental | 15 |
| La carga se puede repetir sin cambiar los resultados | 10 |
| Calidad: las filas inválidas quedan en cuarentena con su motivo | 15 |
| Problemas de datos encontrados y reportados sin que nadie los señalara | 20 |
| Métrica 1 correcta | 10 |
| Métrica 2 correcta | 10 |
| Bono: bundle, pruebas y job programado | 10 |
| README: decisiones explicadas con argumentos | 10 |

Se aprueba con 80 puntos.
