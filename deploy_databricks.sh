#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_DIR="$SCRIPT_DIR/src/databricks"

erro() {
  printf 'Erro: %s\n' "$1" >&2
  exit 1
}

pedir_obrigatorio() {
  local mensagem="$1"
  local valor=""
  while [[ -z "$valor" ]]; do
    read -r -p "$mensagem: " valor
  done
  printf '%s' "$valor"
}

pedir_com_padrao() {
  local mensagem="$1"
  local padrao="$2"
  local valor=""
  read -r -p "$mensagem [$padrao]: " valor
  printf '%s' "${valor:-$padrao}"
}

confirmar() {
  local mensagem="$1"
  local resposta=""
  read -r -p "$mensagem [S/n]: " resposta
  [[ -z "$resposta" || "$resposta" =~ ^[Ss]$ ]]
}

mostrar_ajuda() {
  cat <<'EOF'
Uso:
  ./deploy_databricks.sh

O assistente solicita:
  - host do workspace Databricks;
  - nome local do profile da CLI;
  - catálogo do Unity Catalog;
  - ID do SQL Warehouse;
  - janela da primeira carga.

O catálogo deve ser criado previamente pela interface do Databricks.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  mostrar_ajuda
  exit 0
fi

[[ $# -eq 0 ]] || erro "argumento desconhecido: $1"
command -v databricks >/dev/null 2>&1 ||
  erro "Databricks CLI não encontrada. Instale a versão 0.205 ou superior."
[[ -f "$BUNDLE_DIR/databricks.yml" ]] ||
  erro "databricks.yml não encontrado em $BUNDLE_DIR"

printf '\nDeploy do case iFood no Databricks Free\n\n'
databricks version

WORKSPACE_HOST="$(
  pedir_obrigatorio \
    "Host do workspace, sem /browse ou ?o= (ex.: https://dbc-xxxx.cloud.databricks.com)"
)"
WORKSPACE_HOST="${WORKSPACE_HOST%/}"
[[ "$WORKSPACE_HOST" == https://* ]] ||
  erro "o host deve começar com https://"
HOST_SEM_PROTO="${WORKSPACE_HOST#https://}"
[[ "$HOST_SEM_PROTO" != */* ]] ||
  erro "informe somente o host, sem caminhos ou parâmetros"

PROFILE="$(pedir_obrigatorio "Nome local do profile da Databricks CLI")"
CATALOG="$(pedir_com_padrao "Catálogo existente no Unity Catalog" "case_ifood")"

printf '\nO navegador será aberto para autenticação OAuth.\n'
databricks auth login \
  --host "$WORKSPACE_HOST" \
  --profile "$PROFILE"

printf '\nIdentidade autenticada:\n'
databricks current-user me --profile "$PROFILE"

if ! databricks catalogs get "$CATALOG" --profile "$PROFILE" >/dev/null 2>&1; then
  erro "catálogo '$CATALOG' não encontrado. Crie-o como Standard com Default Storage."
fi

printf '\nSQL Warehouses disponíveis:\n'
databricks warehouses list --profile "$PROFILE"
WAREHOUSE_ID="$(pedir_obrigatorio "ID do SQL Warehouse que será usado pelo dashboard")"

if ! databricks warehouses get "$WAREHOUSE_ID" --profile "$PROFILE" >/dev/null 2>&1; then
  erro "SQL Warehouse '$WAREHOUSE_ID' não encontrado nesse workspace"
fi

INICIO="$(pedir_com_padrao "Mês inicial da primeira carga" "2023-01")"
FIM="$(pedir_com_padrao "Mês final da primeira carga" "2023-05")"

[[ "$INICIO" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]] ||
  erro "mês inicial inválido; use AAAA-MM"
[[ "$FIM" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ || "$FIM" == "auto" ]] ||
  erro "mês final inválido; use AAAA-MM ou auto"
[[ "$FIM" == "auto" || "$INICIO" < "$FIM" || "$INICIO" == "$FIM" ]] ||
  erro "o mês inicial não pode ser posterior ao mês final"

printf '\nResumo\n'
printf '  workspace: %s\n' "$WORKSPACE_HOST"
printf '  profile: %s\n' "$PROFILE"
printf '  catálogo: %s\n' "$CATALOG"
printf '  SQL Warehouse: %s\n' "$WAREHOUSE_ID"
printf '  primeira carga: %s a %s\n\n' "$INICIO" "$FIM"

confirmar "Continuar com validate e deploy?" || {
  printf 'Operação cancelada.\n'
  exit 0
}

BUNDLE_ARGS=(
  -t free
  --profile "$PROFILE"
  --var="catalog=$CATALOG"
  --var="sql_warehouse_id=$WAREHOUSE_ID"
)

cd "$BUNDLE_DIR"

databricks bundle validate "${BUNDLE_ARGS[@]}"
databricks bundle deploy "${BUNDLE_ARGS[@]}"

printf '\nDeploy concluído.\n'

if confirmar "Executar quality_setup e a primeira carga agora?"; then
  databricks bundle run quality_setup "${BUNDLE_ARGS[@]}"
  databricks bundle run ingestion \
    --params "inicio=$INICIO,fim=$FIM" \
    "${BUNDLE_ARGS[@]}"
  printf '\nPrimeira carga concluída.\n'
else
  printf 'Recursos implantados sem executar a primeira carga.\n'
fi

printf '\nRecursos do Bundle:\n'
databricks bundle summary "${BUNDLE_ARGS[@]}"
