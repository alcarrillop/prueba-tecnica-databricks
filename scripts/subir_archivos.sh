#!/usr/bin/env bash
# Sube archivos CSV al Volume landing del proyecto, sin sobrescribir nunca.
#
# Uso:
#   scripts/subir_archivos.sh --profile DEFAULT [--target dev] archivo.csv [archivo.csv ...]
#
# Ejemplo (fase 1):
#   scripts/subir_archivos.sh --profile DEFAULT \
#     datos/departamentos.csv datos/cargos.csv datos/empleados_contratados_lote1.csv
#
# Qué hace:
#   - Lee catálogo y esquema del bundle (databricks bundle validate), así no se repiten aquí.
#   - Crea el esquema y el Volume landing solo si no existen.
#   - Ubica cada archivo en la carpeta de su entidad:
#       departamentos.csv            -> landing/departamentos/
#       cargos.csv                   -> landing/cargos/
#       empleados_contratados_*.csv  -> landing/empleados/
#   - Si el archivo no está en el Volume, lo sube y comprueba que la copia sea idéntica.
#   - Si ya está y es idéntico (byte a byte), no hace nada.
#   - Si ya está con otro contenido, NO lo sobrescribe: lo reporta y el script termina con error.
#   - Avisa (sin bloquear) si el archivo no es UTF-8 válido o si su contenido ya está en
#     la carpeta con otro nombre.
#
# Requiere: databricks CLI, jq, iconv, cmp. Los temporales van a .tmp/ del proyecto.

set -euo pipefail

uso() {
  sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
}

PERFIL=""
TARGET="dev"
ARCHIVOS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--profile) PERFIL="${2:-}"; shift 2 ;;
    -t|--target) TARGET="${2:-}"; shift 2 ;;
    -h|--help) uso; exit 0 ;;
    -*) echo "Opción desconocida: $1" >&2; uso >&2; exit 2 ;;
    *) ARCHIVOS+=("$1"); shift ;;
  esac
done

if [[ -z "$PERFIL" ]]; then
  echo "Falta --profile <perfil de Databricks>." >&2
  exit 2
fi
if [[ ${#ARCHIVOS[@]} -eq 0 ]]; then
  echo "No se indicó ningún archivo." >&2
  uso >&2
  exit 2
fi

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$RAIZ/.tmp/subir_archivos.$$"
mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT

db() { databricks "$@" --profile "$PERFIL"; }

es_no_encontrado() { grep -qiE "does not exist|not found|no such|NOT_FOUND|RESOURCE_DOES_NOT_EXIST" <<<"$1"; }

# --- Catálogo y esquema desde el bundle -------------------------------------------------
VARIABLES="$(cd "$RAIZ" && databricks bundle validate -t "$TARGET" --profile "$PERFIL" -o json 2>/dev/null)" || {
  echo "No se pudo leer el bundle (databricks bundle validate -t $TARGET)." >&2
  exit 1
}
CATALOGO="$(jq -r '.variables.catalogo.value' <<<"$VARIABLES")"
ESQUEMA="$(jq -r '.variables.esquema.value' <<<"$VARIABLES")"
VOLUMEN="landing"
BASE="dbfs:/Volumes/$CATALOGO/$ESQUEMA/$VOLUMEN"
echo "Destino: $BASE (target $TARGET, perfil $PERFIL)"

# --- Esquema y Volume solo si no existen ------------------------------------------------
if ! salida="$(db schemas get "$CATALOGO.$ESQUEMA" 2>&1)"; then
  if es_no_encontrado "$salida"; then
    echo "Creando esquema $CATALOGO.$ESQUEMA"
    db schemas create --json "$(jq -n --arg c "$CATALOGO" --arg e "$ESQUEMA" \
      '{name: $e, catalog_name: $c, comment: "Prueba técnica de talento humano: tablas bronce, plata, cuarentena y oro."}')" >/dev/null
  else
    echo "$salida" >&2
    exit 1
  fi
fi
if ! salida="$(db volumes read "$CATALOGO.$ESQUEMA.$VOLUMEN" 2>&1)"; then
  if es_no_encontrado "$salida"; then
    echo "Creando Volume $CATALOGO.$ESQUEMA.$VOLUMEN"
    db volumes create --json "$(jq -n --arg c "$CATALOGO" --arg e "$ESQUEMA" --arg v "$VOLUMEN" \
      '{catalog_name: $c, schema_name: $e, name: $v, volume_type: "MANAGED", comment: "Archivos CSV tal como llegan del sistema viejo, una carpeta por entidad. No se sobrescriben."}')" >/dev/null
  else
    echo "$salida" >&2
    exit 1
  fi
fi

carpeta_de() {
  case "$1" in
    departamentos.csv) echo "departamentos" ;;
    cargos.csv) echo "cargos" ;;
    empleados_contratados_*.csv) echo "empleados" ;;
    *) return 1 ;;
  esac
}

# Lista los nombres de archivo de una carpeta del Volume (vacío si la carpeta no existe).
listar() {
  local salida
  if salida="$(db fs ls "$1" -o json 2>&1)"; then
    jq -r '.[] | select(.is_directory | not) | .name' <<<"$salida" | sed 's#.*/##'
  elif es_no_encontrado "$salida"; then
    return 0
  else
    echo "$salida" >&2
    return 1
  fi
}

SUBIDOS=0
IGUALES=0
ERRORES=0

for local_path in "${ARCHIVOS[@]}"; do
  nombre="$(basename "$local_path")"
  echo
  echo "== $nombre"

  if [[ ! -f "$local_path" ]]; then
    echo "  ERROR: no existe el archivo local $local_path"
    ERRORES=$((ERRORES + 1))
    continue
  fi
  if ! carpeta="$(carpeta_de "$nombre")"; then
    echo "  ERROR: no sé a qué entidad pertenece (se esperan departamentos.csv, cargos.csv o empleados_contratados_*.csv)"
    ERRORES=$((ERRORES + 1))
    continue
  fi
  if ! iconv -f UTF-8 -t UTF-8 "$local_path" >/dev/null 2>&1; then
    echo "  AVISO: no es UTF-8 válido; se sube igual y queda para el reporte de calidad"
  fi

  dir_destino="$BASE/$carpeta"
  destino="$dir_destino/$nombre"
  existentes="$(listar "$dir_destino")" || { ERRORES=$((ERRORES + 1)); continue; }

  if grep -qxF "$nombre" <<<"$existentes"; then
    db fs cp "$destino" "$TMP/remoto_$nombre" >/dev/null
    if cmp -s "$local_path" "$TMP/remoto_$nombre"; then
      echo "  Ya está en $destino con el mismo contenido: no se hace nada"
      IGUALES=$((IGUALES + 1))
    else
      echo "  CONFLICTO: $destino ya existe con OTRO contenido. No se sobrescribe."
      echo "             local: $(wc -c <"$local_path" | tr -d ' ') bytes; Volume: $(wc -c <"$TMP/remoto_$nombre" | tr -d ' ') bytes"
      ERRORES=$((ERRORES + 1))
    fi
    continue
  fi

  # ¿El mismo contenido ya llegó con otro nombre? Se sube igual (la capa cruda registra
  # todo lo que llega) pero se avisa.
  while IFS= read -r otro; do
    [[ -z "$otro" || "$otro" == "$nombre" ]] && continue
    db fs cp "$dir_destino/$otro" "$TMP/otro_$otro" >/dev/null
    if cmp -s "$local_path" "$TMP/otro_$otro"; then
      echo "  AVISO: el mismo contenido ya está en $dir_destino/$otro"
    fi
  done <<<"$existentes"

  # Sin --overwrite: nunca se reemplaza un archivo existente.
  if ! db fs mkdirs "$dir_destino" >/dev/null || ! db fs cp "$local_path" "$destino" >/dev/null; then
    echo "  ERROR: no se pudo subir a $destino"
    ERRORES=$((ERRORES + 1))
    continue
  fi
  db fs cp "$destino" "$TMP/verificacion_$nombre" >/dev/null
  if cmp -s "$local_path" "$TMP/verificacion_$nombre"; then
    echo "  Subido a $destino (copia verificada byte a byte)"
    SUBIDOS=$((SUBIDOS + 1))
  else
    echo "  ERROR: la copia en $destino no coincide con el archivo local"
    ERRORES=$((ERRORES + 1))
  fi
done

echo
echo "Resumen: $SUBIDOS subidos, $IGUALES ya estaban idénticos, $ERRORES con error o conflicto."
[[ $ERRORES -eq 0 ]]
