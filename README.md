# Pipeline de talento humano en Databricks

Solución a la prueba técnica de ingeniero de datos descrita en [enunciado.md](enunciado.md): recibir los CSV del sistema viejo, dejarlos en tablas confiables y responder las dos preguntas de talento humano.

> Este README se va completando por fases. La sección **Cómo correrlo** se agrega cuando el pipeline esté construido, y los problemas del lote 2 cuando llegue ese lote.

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

### Pruebas

Las pruebas usan pytest con Databricks Connect sobre cómputo serverless, para probar el motor real (modo ANSI, collations, zona horaria). Las que necesitan tablas las crean en el mismo esquema con el prefijo `test_` y las borran al terminar.

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
| Contrataciones en el borde de año o de trimestre | 6 | id 1237 `2021-01-01T04:23:19Z` | Las métricas calculan en UTC; en la zona horaria de Bogotá esta fila caería en 2020 |
| Orden alfabético que distingue mayúsculas | 2 cargos | "Analista Financiero" ordenaría antes que "Analista de Datos" | Las métricas ordenan sin distinguir mayúsculas ni tildes |

Lo que se revisó y está bien: codificación UTF-8 válida y sin caracteres dañados, número de columnas correcto en todas las líneas, ids de departamentos y cargos en secuencia y sin nombres repetidos, y nombres de empleados sin caracteres raros. Hay muchos nombres repetidos con ids distintos (por ejemplo, 7 "Daniela López"), pero ninguno coincide además en fecha, departamento y cargo: son homónimos, no duplicados, y no se deduplica por nombre.

## Cómo se construyó

El trabajo lo hizo un equipo de subagentes de Claude Code definidos en [.claude/agents/](.claude/agents/): `ingesta`, `calidad` y `sql` construyen cada sección, y `entrevistador` califica la entrega con la rúbrica sin confiar en lo que reportan los demás. Las decisiones que el enunciado no aclara se le consultaron al responsable de la prueba antes de construir.
