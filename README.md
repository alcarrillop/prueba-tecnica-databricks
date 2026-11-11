# Pipeline de talento humano en Databricks

Solución a la prueba técnica de ingeniero de datos descrita en [enunciado.md](enunciado.md): recibir los CSV del sistema viejo, dejarlos en tablas confiables y responder las dos preguntas de talento humano.

> Este README se va completando por fases: las métricas y los problemas del lote 2 se agregan cuando estén listos.

## Cómo correrlo

Requisitos: [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) con un perfil autenticado (aquí `DEFAULT`) y, para las pruebas, [uv](https://docs.astral.sh/uv/).

```bash
# 1. Validar y desplegar el bundle en desarrollo
databricks bundle validate -t dev --profile DEFAULT
databricks bundle deploy   -t dev --profile DEFAULT

# 2. Subir los archivos que "llegan" (crea el esquema y el Volume si no existen; nunca sobrescribe)
scripts/subir_archivos.sh --profile DEFAULT datos/departamentos.csv datos/cargos.csv datos/empleados_contratados_lote1.csv

# 3. Correr el pipeline
databricks bundle run pipeline_talento -t dev --profile DEFAULT

# 4. Cuando llega el lote 2: subirlo y volver a correr
scripts/subir_archivos.sh --profile DEFAULT datos/empleados_contratados_lote2.csv
databricks bundle run pipeline_talento -t dev --profile DEFAULT
```

Para comprobar que la carga se puede repetir, basta con correr el paso 3 otra vez sin archivos nuevos: `DESCRIBE HISTORY` de las tablas no muestra versiones nuevas.

Pruebas automáticas (corren contra Databricks serverless con Databricks Connect):

```bash
uv sync
uv run pytest                   # suite completa (unos 20 minutos): crea y borra tablas test_* en el esquema
uv run pytest -m "not tablas"   # solo las pruebas sobre DataFrames en memoria
```

## Arquitectura

Todo vive en el esquema `workspace.prueba_tecnica` y se organiza en capas:

```
datos/*.csv
   │  scripts/subir_archivos.sh (sube solo los archivos indicados, nunca sobrescribe)
   ▼
Volume landing/<entidad>/<archivo>.csv
   │  Auto Loader (availableNow + checkpoint): solo archivos nuevos
   ▼
bronce_departamentos · bronce_cargos · bronce_empleados      (crudo: todo texto, con archivo y línea de origen)
   │  reglas de calidad
   ├──────────────► cuarentena                                (filas rechazadas, con todos sus motivos)
   ▼
plata_departamentos · plata_cargos · plata_empleados          (limpio y tipado)
   │  SQL
   ▼
oro_*                                                         (Métrica 1 y Métrica 2)
```

El pipeline corre como un solo job (`pipeline_talento`) con tres tareas en cadena (ingesta → calidad → métricas), desplegado con un bundle de Databricks que define los ambientes de desarrollo y producción.

## Decisiones y por qué

### Ingesta

| Decisión | Por qué |
|---|---|
| **Auto Loader con checkpoint y `trigger(availableNow=True)`** | El checkpoint recuerda qué archivos ya se procesaron: cuando llega el lote 2 solo se lee el lote 2 (incremental), y una corrida sin archivos nuevos no escribe nada (idempotente). Se descartó `COPY INTO` porque partir cada archivo en líneas numeradas no es natural ahí, y Lakeflow Declarative Pipelines porque ataría todas las tablas a un mismo pipeline. |
| **Cada archivo se lee como texto y se guarda el número de línea y la línea original** | El lector CSV estándar no da número de línea, se salta las líneas vacías y descarta en silencio las columnas de más. La cuarentena necesita decir de qué archivo y línea viene cada fila rechazada. |
| **Cada línea se parte por comas (`split(linea, ',', -1)`), no con un parser CSV** | Con `from_csv`, un campo vacío y uno que falta quedan los dos en NULL, y las columnas de más se pierden sin aviso. Con `split`, un campo vacío queda como texto vacío, uno que falta queda en NULL y lo que sobra sigue en la línea original. A cambio, las comillas no se interpretan: un valor entre comillas con una coma adentro produciría columnas de más y la fila iría a cuarentena, no se perdería. Los archivos recibidos no usan comillas. |
| **De cada línea solo se quita el terminador (`\r\n` o `\n`)** | Los archivos vienen con finales de línea Windows. Se comprobó que ningún valor conserva el `\r`. Una línea vacía en medio del archivo sí se guarda, para que calidad la vea. |
| **Columnas de trazabilidad en cada fila cruda** | `_archivo_origen` y `_numero_linea` identifican la fila; `_linea_original` es el texto exacto; `_archivo_modificado_en` es la hora de subida al Volume (no la de exportación del sistema viejo); `_ingestado_en` es el momento de la corrida. |
| **Cada archivo se lee entero en memoria** (`wholetext`) | Es lo que permite numerar las líneas. Sirve para archivos de KB o MB como estos; para archivos de GB habría que cambiar de lector. |
| **Todas las columnas de la capa bronce son texto** | Ningún valor se pierde al convertir tipos. Un valor inválido llega intacto a calidad, que decide qué hacer con él y deja el motivo. |
| **Las tablas bronce solo admiten agregar filas** (`delta.appendOnly`) | Son el registro fiel de lo que llegó. Nada se reescribe ni se borra. |
| **El esquema y los Volumes los crea el código (`IF NOT EXISTS`), no el bundle** | Si alguien destruye el bundle, los datos y los checkpoints no se borran con él. |
| **La subida al Volume la hace un script local que solo sube los archivos que se le indican** | Así se simula que el lote 2 llega días después. Si llega un archivo con el mismo nombre pero otro contenido, no se sobrescribe: el script termina con error. Si llega el mismo contenido con otro nombre, se carga, y calidad marca los ids repetidos. |
| **Un solo esquema para dev y prod** | La prueba usa un único esquema. Producción queda definida en el bundle pero no se despliega; apuntarla a otro esquema es cambiar una línea. |
| **Cómputo serverless en modo estándar** | Es un proceso por lotes sin plazo estricto; el modo estándar es el más económico. |
| **Job programado a diario a las 06:00 UTC** | Queda pausado en desarrollo y activo en producción. |

### Calidad

| Decisión | Por qué |
|---|---|
| **Vacío = NULL, texto vacío o texto con solo espacios** (incluye tabulador y espacio duro). Los textos `NULL` o `N/A` no cuentan como vacío. | Cubre todas las formas en que un campo obligatorio puede llegar sin dato. |
| **Ids: solo dígitos y mayor o igual a 1** (`007` se acepta como 7) | Un id negativo o en cero no identifica a nadie; los ceros a la izquierda no cambian el valor. |
| **Fecha estricta: `AAAA-MM-DDTHH:MM:SSZ` y que exista en el calendario** | Es el formato que define el enunciado. Las fechas en otro formato (`27/07/2021 16:02`, `20210727`, `ayer`) no se rescatan, porque habría que suponer la zona horaria y el orden de día y mes. Una fecha imposible (`2021-02-30`) va a cuarentena con un motivo distinto y no se "acomoda" al día siguiente. |
| **Fechas futuras o anteriores a 1950 van a cuarentena** | No son fechas de contratación plausibles. |
| **Se guardan todos los motivos de rechazo de una fila**, uno por causa | Quien revise la cuarentena ve todo lo que hay que corregir de una vez. |
| **Departamento y cargo se validan contra las tablas plata** | Un empleado no puede apuntar a un departamento que a su vez fue rechazado. |
| **Filas repetidas exactas: se conserva la primera (por archivo y línea) y las demás van a cuarentena** | Son la misma contratación. Mandarlas todas a cuarentena perdería contrataciones reales; conservarlas todas las contaría dos veces. |
| **Mismo id con datos distintos: todas las versiones van a cuarentena** | No hay forma de saber cuál es la verdadera. |
| **El resultado no depende de cómo se partieron los lotes** | Procesar el lote 1 y luego el 2 da lo mismo que procesarlos juntos. Si el lote 2 trae otra versión de un id que ya estaba limpio, las dos van a cuarentena (la del lote 1 se retira de plata), y por eso las métricas pueden cambiar hacia atrás. |
| **Espacios sobrantes en los bordes de los textos se quitan**, en un paso propio que registra qué fila tocó (`_correcciones`) | Sin esto, " Servicio al Cliente  " ordena antes que "Cadena de Suministro". El valor original sigue en la capa bronce. |
| **Una sola tabla de cuarentena con una columna `entidad`** | Un solo lugar para revisar los rechazos de las tres tablas. |
| **`fecha_contratacion` se guarda como TIMESTAMP y las métricas calculan año y trimestre en UTC** | Las fechas vienen en UTC. Calcular en la zona horaria de la sesión movería contrataciones de año o de trimestre. |
| **Reprocesar la cuarentena cuando lleguen correcciones queda fuera de alcance** | Se documenta como siguiente paso. |

### Métricas

| Decisión | Por qué |
|---|---|
| **Se calculan sobre las tablas plata, nunca sobre bronce** | Las filas en cuarentena no deben contar. |
| **Dos tablas con las columnas exactas del enunciado (`oro_contratados_trimestre_2021` y `oro_departamentos_sobre_promedio_2021`), más una vista `_ordenada` de cada una** | Una tabla Delta no garantiza el orden de las filas al leerla. La vista trae el `ORDER BY` oficial, así que nadie tiene que recordar la collation ni el desempate. |
| **Se recalculan completas en cada corrida (`CREATE OR REPLACE TABLE ... AS SELECT`)** | Calidad puede retirar filas de plata cuando llega un lote nuevo, así que un cálculo incremental quedaría desactualizado. Los datos son pocos y el reemplazo es atómico. No usan la hora actual ni nada aleatorio: repetir da el mismo resultado. |
| **Año y trimestre se calculan comparando `fecha_contratacion` con instantes fijos escritos en UTC** (por ejemplo `TIMESTAMP'2021-04-01T00:00:00Z'`) | Una comparación entre instantes es exacta con cualquier zona horaria de la sesión. `year(fecha)` directo cambia con la zona de la sesión, y `convert_timezone` se descartó porque se equivoca en la hora que se repite al cambiar el horario de verano. Se comprobó corriendo las métricas con la sesión en UTC y en Bogotá, y en el warehouse: el resultado es idéntico. |
| **Métrica 1: solo las combinaciones con al menos una contratación en 2021** | En el ejemplo del enunciado los ceros aparecen en trimestres, no en filas completas. Listar las 300 combinaciones posibles inventaría combinaciones que quizá no existen. |
| **Métrica 2: el promedio divide el total de 2021 entre todos los departamentos, y "más que el promedio" es estrictamente mayor** | Es la lectura literal de "el promedio de todos los departamentos": los que no contrataron cuentan como 0. Se compara con enteros (`contratados × departamentos > total`) para no depender del redondeo. Con estos datos los 12 departamentos contrataron, así que dividir solo entre los que contrataron daría lo mismo. |
| **Orden alfabético con la collation del español (`COLLATE ES`)** | El orden binario pone "Analista Financiero" antes que "Analista de Datos" y manda al final las palabras que empiezan con tilde. Con `ES`, las mayúsculas y las tildes solo desempatan, la ñ va después de la n y el orden siempre es el mismo. |
| **Desempate de la Métrica 2: contratados de mayor a menor, luego nombre del departamento, luego id** | Garantiza un orden único y coherente con el de la Métrica 1. |
| **El `id` de la Métrica 2 es el id del departamento** | Es lo que pide el enunciado. Los ids del ejemplo del enunciado son ilustrativos. |
| **Las métricas corren como tareas SQL en el warehouse** | La sección pide SQL: el repo y el job muestran SQL puro. Las consultas reciben catálogo, esquema y un prefijo de tabla como parámetros, así que las pruebas las corren sobre tablas pequeñas hechas a mano. |
| **Una tarea de verificación dentro del job** | Hace fallar el job si la suma de los trimestres no cuadra con plata, si algún empleado se pierde en un cruce o si un segundo cálculo, escrito de otra forma, no coincide. Corre después de reemplazar las tablas: si falla, las tablas ya cambiaron, pero el job queda en error. |

### Pruebas

Las pruebas usan pytest con Databricks Connect sobre cómputo serverless, para probar el motor real (modo ANSI, collations, zona horaria). Las que necesitan tablas las crean en el mismo esquema con el prefijo `test_` y las borran al terminar.

## Evidencia: la carga es incremental y se puede repetir

Corridas reales del job en desarrollo. Cada "repetir" es una corrida sin archivos nuevos.

| Momento | `bronce_departamentos` | `bronce_cargos` | `bronce_empleados` | Qué se comprobó |
|---|---|---|---|---|
| Llega el lote 1 | 12 filas (v1) | 25 (v1) | 1408 (v1) | El conteo es igual a las líneas de cada archivo, y el md5 de las líneas guardadas es igual al del archivo local |
| Repetir | sin versión nueva | sin versión nueva | sin versión nueva | `EXCEPT ALL` contra v1 da 0 filas en los dos sentidos |
| Llega el lote 2 | sin versión nueva | sin versión nueva | 2020 (v2, +612) | Solo se procesó el lote 2 (612 líneas). Las 1408 filas del lote 1 no cambiaron (`EXCEPT ALL` contra v1 da 0 y conservan su `_ingestado_en`). `cloud_files_state` lista los dos archivos |
| Repetir | sin versión nueva | sin versión nueva | sin versión nueva | `EXCEPT ALL` contra v2 da 0 filas en los dos sentidos |

Calidad y métricas se comprobaron igual sobre el lote 1: una segunda corrida no escribió nada en plata ni en cuarentena, y las tablas oro quedaron idénticas.

## Problemas encontrados en los datos

Revisión hecha sobre los CSV de departamentos, cargos y el lote 1 de empleados (los números de línea cuentan desde 1).

| Problema | Filas | Ejemplos | Qué se hizo |
|---|---|---|---|
| Espacios sobrantes en nombres de departamento | 3 | línea 3 `  Talento Humano`, línea 7 `Tecnología `, línea 10 ` Servicio al Cliente  ` | Se quitan en un paso registrado; el original queda en bronce |
| `nombre` vacío | 10 | id 29: `29,,2021-10-22T05:54:58Z,7,5` | Cuarentena |
| `id_cargo` vacío | 6 | id 72: `72,Nicolás Gómez,2021-01-07T10:41:02Z,3,` | Cuarentena |
| `fecha_contratacion` vacía | 5 | id 171: `171,Paula López,,4,7` | Cuarentena |
| Fecha en otro formato | 3 | id 236 `27/07/2021 16:02`, id 1090 `20210727`, id 877 `ayer` | Cuarentena |
| Fecha que no existe en el calendario | 3 | id 323 `2021-02-30T10:00:00Z`, id 782 `2021-13-01T08:00:00Z`, id 1203 `2021-06-31T12:00:00Z` | Cuarentena (un parser laxo las habría movido de trimestre o de año) |
| Departamento que no existe | 7 | `id_departamento` 15 (5 filas) y 99 (2 filas) | Cuarentena. Queda abierta la pregunta al negocio de qué significan |
| Filas repetidas exactas | 7 ids | id 313 en las líneas 315 y 453 | Se conserva una; la copia va a cuarentena |
| Mismo id con datos distintos | 1 id, 2 filas | id 765: cambia fecha y departamento | Las dos versiones a cuarentena |
| Finales de línea Windows (`\r\n`) | todas | los tres archivos | Se verifica que ningún valor conserve el `\r` |
| Contrataciones en el borde de año o de trimestre | varias | id 1237 `2021-01-01T04:23:19Z` caería en 2020 en hora de Bogotá; id 1096 `2022-01-01T01:47:18Z` caería en 2021 | Las métricas calculan año y trimestre en UTC explícito |
| Orden alfabético que distingue mayúsculas | 2 cargos | "Analista Financiero" ordenaría antes que "Analista de Datos" | Las métricas ordenan con la collation del español (`ES`) |

Lo que se revisó y está bien: codificación UTF-8 válida y sin caracteres dañados, número de columnas correcto en todas las líneas (también en el lote 2, que además no usa comillas, lo que importa porque las líneas se parten con `split`), ids de departamentos y cargos en secuencia y sin nombres repetidos, y nombres de empleados sin caracteres raros. Hay muchos nombres repetidos con ids distintos (por ejemplo, 7 "Daniela López"), pero ninguno coincide además en fecha, departamento y cargo: son homónimos, no duplicados, y no se deduplica por nombre.

## Cómo se construyó

El trabajo lo hizo un equipo de subagentes de Claude Code definidos en [.claude/agents/](.claude/agents/): `ingesta`, `calidad` y `sql` construyen cada sección, y `entrevistador` califica la entrega con la rúbrica sin confiar en lo que reportan los demás. Las decisiones que el enunciado no aclara se le consultaron al responsable de la prueba antes de construir.
