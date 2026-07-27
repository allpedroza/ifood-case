#!/usr/bin/env bash

set -euo pipefail

PROFILE=""
CATALOG=""
LANDING_SCHEMA="tlc_data_landing"
VOLUME_NAME="nyc_tlc_landing"
INICIO=""
FIM=""
DRY_RUN=false

erro() {
  printf 'Erro: %s\n' "$1" >&2
  exit 1
}

mostrar_ajuda() {
  cat <<'EOF'
Uso:
  scripts/stage_landing.sh \
    --profile <PROFILE> \
    --catalog case_ifood \
    --inicio 2023-01 \
    --fim 2023-05

Opções:
  --landing-schema <SCHEMA>  Padrão: tlc_data_landing
  --volume <VOLUME>          Padrão: nyc_tlc_landing
  --dry-run                  Mostra os arquivos sem baixar ou enviar
  -h, --help                 Mostra esta ajuda
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --catalog)
      CATALOG="${2:-}"
      shift 2
      ;;
    --landing-schema)
      LANDING_SCHEMA="${2:-}"
      shift 2
      ;;
    --volume)
      VOLUME_NAME="${2:-}"
      shift 2
      ;;
    --inicio)
      INICIO="${2:-}"
      shift 2
      ;;
    --fim)
      FIM="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      mostrar_ajuda
      exit 0
      ;;
    *)
      erro "argumento desconhecido: $1"
      ;;
  esac
done

[[ -n "$PROFILE" ]] || erro "--profile é obrigatório"
[[ -n "$CATALOG" ]] || erro "--catalog é obrigatório"
[[ "$INICIO" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]] ||
  erro "--inicio deve usar AAAA-MM"
if [[ "$FIM" == "auto" ]]; then
  if date -v-1m '+%Y-%m' >/dev/null 2>&1; then
    FIM="$(date -v-1m '+%Y-%m')"
  elif date -d 'last month' '+%Y-%m' >/dev/null 2>&1; then
    FIM="$(date -d 'last month' '+%Y-%m')"
  else
    erro "não foi possível resolver --fim auto neste sistema"
  fi
fi
[[ "$FIM" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]] ||
  erro "--fim deve usar AAAA-MM ou auto"
[[ "$INICIO" < "$FIM" || "$INICIO" == "$FIM" ]] ||
  erro "--inicio não pode ser posterior a --fim"

if [[ "$DRY_RUN" == false ]]; then
  command -v curl >/dev/null 2>&1 || erro "curl não encontrado"
  command -v databricks >/dev/null 2>&1 ||
    erro "Databricks CLI não encontrada"
fi

DESTINO_BASE="dbfs:/Volumes/$CATALOG/$LANDING_SCHEMA/$VOLUME_NAME/tlc"
METADATA_BASE_URL="https://home4.nyc.gov/assets/tlc/downloads/pdf"
TRIP_BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

METADATA_NAMES=(
  "trip_record_user_guide.pdf"
  "data_dictionary_trip_records_yellow.pdf"
  "data_dictionary_trip_records_green.pdf"
  "data_dictionary_trip_records_fhv.pdf"
  "data_dictionary_trip_records_hvfhs.pdf"
  "working_parquet_format.pdf"
  "taxi_zone_lookup.csv"
  "taxi_zones.zip"
)
METADATA_URLS=(
  "$METADATA_BASE_URL/trip_record_user_guide.pdf"
  "$METADATA_BASE_URL/data_dictionary_trip_records_yellow.pdf"
  "$METADATA_BASE_URL/data_dictionary_trip_records_green.pdf"
  "$METADATA_BASE_URL/data_dictionary_trip_records_fhv.pdf"
  "$METADATA_BASE_URL/data_dictionary_trip_records_hvfhs.pdf"
  "$METADATA_BASE_URL/working_parquet_format.pdf"
  "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
  "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
)
TRIP_TYPES=("yellow" "green" "fhv" "fhvhv")

arquivo_remoto_existe() {
  local diretorio="$1"
  local nome="$2"
  databricks fs ls "$diretorio" --profile "$PROFILE" 2>/dev/null |
    awk '{print $NF}' |
    grep -Fqx "$nome"
}

validar_parquet() {
  local arquivo="$1"
  local assinatura_inicial=""
  local assinatura_final=""
  assinatura_inicial="$(head -c 4 "$arquivo")"
  assinatura_final="$(tail -c 4 "$arquivo")"
  [[ "$assinatura_inicial" == "PAR1" && "$assinatura_final" == "PAR1" ]]
}

baixar_e_enviar() {
  local nome="$1"
  local url="$2"
  local diretorio_remoto="$3"
  local parquet="$4"
  local arquivo_local="$PASTA_TEMPORARIA/$nome"

  if [[ "$DRY_RUN" == true ]]; then
    printf 'planejado: %s -> %s/%s\n' "$url" "$diretorio_remoto" "$nome"
    return
  fi

  if arquivo_remoto_existe "$diretorio_remoto" "$nome"; then
    printf 'já existe no Volume, pulando: %s\n' "$nome"
    return
  fi

  printf 'baixando: %s\n' "$nome"
  curl \
    --fail \
    --location \
    --retry 3 \
    --retry-all-errors \
    --retry-delay 5 \
    --output "$arquivo_local" \
    "$url"

  [[ -s "$arquivo_local" ]] || erro "arquivo vazio: $nome"
  if [[ "$parquet" == true ]] && ! validar_parquet "$arquivo_local"; then
    erro "assinatura Parquet inválida: $nome"
  fi

  databricks fs mkdir "$diretorio_remoto" --profile "$PROFILE"
  databricks fs cp \
    "$arquivo_local" \
    "$diretorio_remoto/$nome" \
    --overwrite \
    --profile "$PROFILE"
  rm "$arquivo_local"
  printf 'enviado: %s\n' "$nome"
}

PASTA_TEMPORARIA="$(mktemp -d "${TMPDIR:-/tmp}/ifood-landing.XXXXXX")"
trap 'rm -rf "$PASTA_TEMPORARIA"' EXIT

printf '\nMetadados oficiais\n'
for indice in "${!METADATA_NAMES[@]}"; do
  baixar_e_enviar \
    "${METADATA_NAMES[$indice]}" \
    "${METADATA_URLS[$indice]}" \
    "$DESTINO_BASE/_metadata" \
    false
done

ano_inicial="${INICIO%-*}"
mes_inicial="${INICIO#*-}"
ano_final="${FIM%-*}"
mes_final="${FIM#*-}"
ano=$((10#$ano_inicial))
mes=$((10#$mes_inicial))
limite=$((10#$ano_final * 12 + 10#$mes_final))

printf '\nParquets de viagens\n'
for tipo in "${TRIP_TYPES[@]}"; do
  ano_atual=$ano
  mes_atual=$mes
  while ((ano_atual * 12 + mes_atual <= limite)); do
    printf -v ano_mes '%04d-%02d' "$ano_atual" "$mes_atual"
    printf -v mes_formatado '%02d' "$mes_atual"
    nome="${tipo}_tripdata_${ano_mes}.parquet"
    baixar_e_enviar \
      "$nome" \
      "$TRIP_BASE_URL/$nome" \
      "$DESTINO_BASE/$tipo/$ano_atual/$mes_formatado" \
      true

    if ((mes_atual == 12)); then
      ano_atual=$((ano_atual + 1))
      mes_atual=1
    else
      mes_atual=$((mes_atual + 1))
    fi
  done
done

printf '\nLanding preparada em %s\n' "$DESTINO_BASE"
