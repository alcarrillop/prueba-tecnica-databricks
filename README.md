# Pipeline de talento humano en Databricks

Solución a la prueba técnica de ingeniero de datos descrita en [enunciado.md](enunciado.md): recibir los CSV del sistema viejo, dejarlos en tablas confiables y responder las dos preguntas de talento humano.

## Resultados

Con los dos lotes procesados hay 1481 contrataciones válidas en 2021.

**Métrica 2: departamentos que contrataron más que el promedio en 2021.** El promedio es 1481 / 12 = 123,42.

| id | departamento | contratados |
|---|---|---|
| 7 | Tecnología | 272 |
| 4 | Ventas | 264 |
| 1 | Cadena de Suministro | 197 |

El siguiente es Operaciones, con 122, a 1,42 del promedio. Es el resultado más sensible a las decisiones de calidad. Se comprobó que contar las copias, rescatar las fechas en otro formato o contar las filas sin nombre o sin cargo no lo hacen entrar.

**Métrica 1: contrataciones de 2021 por departamento, cargo y trimestre.** Tiene 285 combinaciones de departamento y cargo, con Q1 366, Q2 366, Q3 363 y Q4 386. Las primeras filas:

| departamento | cargo | Q1 | Q2 | Q3 | Q4 |
|---|---|---|---|---|---|
| Cadena de Suministro | Abogado | 3 | 3 | 4 | 4 |
| Cadena de Suministro | Agente de Soporte | 1 | 3 | 3 | 5 |
| Cadena de Suministro | Analista de Datos | 4 | 2 | 4 | 0 |
| Cadena de Suministro | Analista Financiero | 2 | 2 | 2 | 2 |
| … | | | | | |

Las dos tablas completas, en el orden pedido:

```sql
SELECT * FROM workspace.prueba_tecnica.oro_contratados_trimestre_2021_ordenada;
SELECT * FROM workspace.prueba_tecnica.oro_departamentos_sobre_promedio_2021_ordenada;
```

Cómo se verificaron:
- Se calcularon una segunda vez de forma independiente, y coinciden celda por celda.
- Se corrieron con la sesión en Bogotá, Tokio y Nueva York, y no hubo diferencias. Las 32 contrataciones a menos de 24 horas de un cambio de año o de trimestre caen en el trimestre UTC correcto.
- Dos corridas del job dejan las tablas idénticas.

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

Para comprobar que la carga se puede repetir, basta con correr el paso 3 otra vez sin archivos nuevos: `DESCRIBE HISTORY` de las tablas bronce, plata y cuarentena no muestra versiones nuevas. Las tablas oro se reconstruyen completas en cada corrida, así que sí crean una versión nueva, pero con el mismo contenido (`EXCEPT ALL` contra la versión anterior da 0 filas).

Pruebas automáticas (corren contra Databricks serverless con Databricks Connect):

```bash
uv sync
uv run pytest                   # suite completa: 53 pruebas, unos 24 minutos; crea y borra tablas test_* en el esquema
uv run pytest -m "not tablas"   # solo las 40 pruebas sobre DataFrames en memoria, unos 3 minutos
```

Usan el perfil `DEFAULT` (se cambia con `DATABRICKS_CONFIG_PROFILE`). Cubren cada regla de calidad, la carga incremental entre lotes (copias, conflictos con retirada de filas de plata, corridas repetidas e interrumpidas) y las dos métricas: bordes de trimestre en UTC con la sesión en Bogotá y Nueva York, el promedio que cuenta los departamentos sin contrataciones, la comparación estricta, el orden en español y el recorrido completo bronce → plata → oro.

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
| **Cada línea se parte por comas (`split(linea, ',', -1)`), no con un parser CSV** | Con `from_csv`, un campo vacío y uno que falta quedan los dos en NULL, y las columnas de más se pierden sin aviso. Con `split`, un campo vacío queda como texto vacío, uno que falta queda en NULL y lo que sobra sigue en la línea original. A cambio, las comillas no se interpretan, y calidad manda a cuarentena cualquier línea que las traiga (ver Calidad). Los archivos recibidos no usan comillas. |
| **De cada línea solo se quita el terminador (`\r\n` o `\n`)** | Los archivos vienen con finales de línea Windows. Se comprobó que ningún valor conserva el `\r`. Una línea vacía en medio del archivo sí se guarda, para que calidad la vea. |
| **Columnas de trazabilidad en cada fila cruda** | `_archivo_origen` y `_numero_linea` identifican la fila; `_linea_original` es el texto exacto; `_archivo_modificado_en` es la hora de subida al Volume (no la de exportación del sistema viejo); `_ingestado_en` es el momento de la corrida. |
| **Cada archivo se lee entero en memoria** (`wholetext`) | Es lo que permite numerar las líneas. Sirve para archivos de KB o MB como estos; para archivos de GB habría que cambiar de lector. |
| **Todas las columnas de la capa bronce son texto** | Ningún valor se pierde al convertir tipos. Un valor inválido llega intacto a calidad, que decide qué hacer con él y deja el motivo. |
| **Las tablas bronce solo admiten agregar filas** (`delta.appendOnly`) | Son el registro fiel de lo que llegó. Nada se reescribe ni se borra. |
| **El esquema y los Volumes los crea el código (`IF NOT EXISTS`), no el bundle** | Si alguien destruye el bundle, los datos y los checkpoints no se borran con él. |
| **La subida al Volume la hace un script local que solo sube los archivos que se le indican** | Así se simula que el lote 2 llega días después. Si llega un archivo con el mismo nombre pero otro contenido, no se sobrescribe: el script termina con error. Si llega el mismo contenido con otro nombre, se carga, y calidad marca los ids repetidos. |
| **Un solo esquema para dev y prod** | La prueba usa un único esquema. Producción queda definida en el bundle pero no se despliega; apuntarla a otro esquema es cambiar una línea. Al compartir esquema, dev y prod también comparten los checkpoints. Si se desplegara prod aquí, habría dos jobs sobre el mismo checkpoint, y `max_concurrent_runs: 1` solo impide corridas simultáneas dentro de un mismo job. Por eso **prod no se debe desplegar en este esquema**: en un caso real se apunta a su propio esquema o catálogo en `databricks.yml`. |
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
| **Las columnas se cuentan igual que en la ingesta (partiendo por comas), y cualquier línea con una comilla doble va a cuarentena (`COMILLAS_NO_SOPORTADAS`)** | El formato del enunciado no usa comillas. Así `52,"Pérez, Ana",...` va a cuarentena con `COLUMNAS_INVALIDAS` y `COMILLAS_NO_SOPORTADAS`, y un valor entrecomillado sin coma, como `"Ana Pérez"`, nunca llega a plata con las comillas puestas. Cuando las columnas no cuadran no se reportan errores de los campos, porque no se sabe qué columna sobra o falta. Los datos entregados no traen comillas: el cambio no movió ninguna fila. |
| **Departamento y cargo se validan contra las tablas plata** | Un empleado no puede apuntar a un departamento que a su vez fue rechazado. |
| **Filas repetidas exactas: se conserva la primera que se cargó (por `_ingestado_en`; si llegaron en la misma corrida, la de menor archivo y línea) y las demás van a cuarentena** | Son la misma contratación. Mandarlas todas a cuarentena perdería contrataciones reales; conservarlas todas las contaría dos veces. Conservar la primera que se cargó hace que una fila que ya está en plata nunca se reemplace cuando llega un lote nuevo. Como las copias son idénticas, lo único que cambiaría con otra regla es el archivo y la línea de origen registrados. |
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

Calidad, con el job completo. `plata_departamentos` (12) y `plata_cargos` (25) no cambiaron en ningún momento.

| Momento | `plata_empleados` | `cuarentena` | Qué se comprobó |
|---|---|---|---|
| Lote 1 | 1365 (v1) | 43 (v1) | bronce 1408 = 1365 + 43; ninguna fila en las dos tablas ni sin destino |
| Repetir | sin versión nueva | sin versión nueva | `EXCEPT ALL` contra v1 da 0 filas |
| Lote 2 | 1945 (v2 retira 2, v3 agrega 582) | 75 (v2, +32) | bronce 2020 = 1945 + 75. Los ids 130 y 881 del lote 1 salieron de plata y están en cuarentena junto con sus versiones del lote 2. Las 43 filas anteriores de la cuarentena conservan su motivo y su `rechazado_en` |
| Repetir | sin versión nueva | sin versión nueva | `EXCEPT ALL` contra la versión anterior da 0 filas en las dos tablas |

Las tablas oro se reconstruyen completas en cada corrida (`CREATE OR REPLACE`), así que cada corrida crea una versión nueva; lo que se comprueba es que el contenido sea idéntico.

Cuarentena final de empleados por motivo (ninguna fila tiene más de un motivo):

| Motivo | Filas |
|---|---|
| `ID_DUPLICADO_COPIA` | 15 |
| `CAMPO_VACIO:nombre` | 12 |
| `CAMPO_VACIO:id_cargo` | 11 |
| `CAMPO_VACIO:fecha_contratacion` | 10 |
| `ID_DUPLICADO_CONFLICTO` | 10 |
| `DEPARTAMENTO_NO_EXISTE` | 9 |
| `FECHA_FORMATO_INVALIDO` | 5 |
| `FECHA_INEXISTENTE` | 3 |
| **Total** | **75** |

## Problemas encontrados en los datos

Los números de línea cuentan desde 1.

### Departamentos, cargos y lote 1

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

### Lote 2

Revisión hecha sobre `empleados_contratados_lote2.csv` (612 líneas: 600 ids nuevos, 1401 a 2000, más 10 ids que ya venían en el lote 1 y 2 repetidos dentro del lote). Todo lo cubren las reglas ya definidas; no hizo falta ninguna regla nueva.

| Problema | Filas | Ejemplos (línea del lote 2, id) | Qué se hizo |
|---|---|---|---|
| Copia exacta de una fila que ya estaba limpia desde el lote 1 | 8 | línea 426, id 1400 (igual a la línea 1408 del lote 1); también los ids 121, 366, 699, 884, 1070, 1081 y 1276 | Se conserva la del lote 1; la del lote 2 va a cuarentena (`ID_DUPLICADO_COPIA`). Contarlas sumaría 8 contrataciones de 2021 que no existen |
| Mismo id que una fila limpia del lote 1, con otros datos | 2 ids | id 881: llega con fecha `2020-08-16` y departamento 2; en el lote 1 tenía `2020-10-31` y departamento 1. Igual el id 130 | Las cuatro versiones van a cuarentena (`ID_DUPLICADO_CONFLICTO`) y las dos del lote 1 se **retiran de plata**. Es la regla "el resultado no depende de cómo se partieron los lotes" aplicada con datos reales. Las cuatro son de 2020: no cambian las métricas |
| Mismo id con datos distintos dentro del lote 2 | 2 ids, 4 filas | id 1749 en las líneas 354 y 355 (cambian fecha y departamento); id 1848 en las líneas 169 y 456 | Todas a cuarentena (`ID_DUPLICADO_CONFLICTO`) |
| Fecha en otro formato | 2 | línea 128, id 1527: `07-27-2021` (mes-día-año); línea 416, id 1809: `2021/09/15 09:30:00` (sin zona horaria) | Cuarentena (`FECHA_FORMATO_INVALIDO`). El lote 1 traía `27/07/2021` (día/mes) y el lote 2 `07-27-2021` (mes-día): rescatarlas exigiría adivinar el orden del día y el mes, lo que confirma la decisión de no hacerlo |
| `fecha_contratacion` vacía | 5 | línea 17, id 1417 | Cuarentena |
| `id_cargo` vacío | 5 | línea 47, id 1447 | Cuarentena |
| `nombre` vacío | 2 | línea 290, id 1687 | Cuarentena |
| Departamento que no existe | 2 | línea 64, id 1464: departamento 99; línea 142, id 1541: departamento 15 | Cuarentena. Son los mismos códigos 15 y 99 del lote 1 |
| Contrataciones en el borde de trimestre | 9 | id 1856 `2021-03-31T22:00:38Z`, id 1702 `2021-12-31T18:26:07Z` | Las métricas calculan en UTC |

Las diferencias de los ids 881, 130, 1749 y 1848 podrían ser correcciones del sistema viejo, y en ese caso la regla sería "gana la última versión". Se descartó porque no hay forma de saberlo desde los datos, y las 8 copias idénticas apuntan más a un reenvío que a correcciones.

### Lo que se revisó y está bien

Codificación UTF-8 válida y sin caracteres dañados, número de columnas correcto en todas las líneas (también en el lote 2, que además no usa comillas, lo que importa porque las líneas se parten con `split`), ids de departamentos y cargos en secuencia y sin nombres repetidos, y nombres de empleados sin caracteres raros. Hay muchos nombres repetidos con ids distintos (por ejemplo, 7 "Daniela López"), pero ninguno coincide además en fecha, departamento y cargo: son homónimos, no duplicados, y no se deduplica por nombre.

## Cómo se construyó

El trabajo lo hizo un equipo de subagentes de Claude Code definidos en [.claude/agents/](.claude/agents/): `ingesta`, `calidad` y `sql` construyen cada sección, y `entrevistador` califica la entrega con la rúbrica sin confiar en lo que reportan los demás. Las decisiones que el enunciado no aclara se le consultaron al responsable de la prueba antes de construir.
