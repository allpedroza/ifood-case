"""
Download dos Trip Records da NYC TLC para um Unity Catalog Volume.

Informe INICIO e FIM no formato "AAAA-MM". O for percorre todos os
meses do intervalo (inclusive), tratando virada de ano automaticamente.
"""

from datetime import date
from pathlib import Path
import argparse
import time
import requests

# PARAMETROS
TIPOS = ["yellow", "green", "fhv", "fhvhv"]   # baixa todas as categorias
BAIXAR_METADADOS = True

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data" 
METADATA_BASE_URL = "https://home4.nyc.gov/assets/tlc/downloads/pdf"
ARTEFATOS_METADATA = [
    ("trip_record_user_guide.pdf", f"{METADATA_BASE_URL}/trip_record_user_guide.pdf"),
    (
        "data_dictionary_trip_records_yellow.pdf",
        f"{METADATA_BASE_URL}/data_dictionary_trip_records_yellow.pdf",
    ),
    (
        "data_dictionary_trip_records_green.pdf",
        f"{METADATA_BASE_URL}/data_dictionary_trip_records_green.pdf",
    ),
    (
        "data_dictionary_trip_records_fhv.pdf",
        f"{METADATA_BASE_URL}/data_dictionary_trip_records_fhv.pdf",
    ),
    (
        "data_dictionary_trip_records_hvfhs.pdf",
        f"{METADATA_BASE_URL}/data_dictionary_trip_records_hvfhs.pdf",
    ),
    (
        "working_parquet_format.pdf",
        f"{METADATA_BASE_URL}/working_parquet_format.pdf",
    ),
    (
        "taxi_zone_lookup.csv",
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
    ),
    (
        "taxi_zones.zip",
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip",
    ),
]
MAX_TENTATIVAS = 6
TIMEOUT = 60             # segundos por request
INTERVALO_REQUISICOES = 1
ESPERA_ENTRE_TENTATIVAS = 60
MESES_RECENTES_PERMITIDOS = 2
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; NYC-TLC-data-pipeline/1.0)"}


def ler_argumentos():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inicio",
        required=True,
        help="Mês inicial no formato AAAA-MM",
    )
    parser.add_argument(
        "--fim",
        required=True,
        help="Mês final no formato AAAA-MM ou 'auto' para o mês anterior",
    )
    parser.add_argument(
        "--destino",
        type=Path,
        required=True,
        help="Caminho obrigatório de um Unity Catalog Volume",
    )
    parser.add_argument(
        "--landing-schema",
        required=True,
        help="Schema Landing configurado pelo Bundle",
    )
    parser.add_argument(
        "--volume-name",
        required=True,
        help="Volume Landing configurado pelo Bundle",
    )
    return parser.parse_args()


def validar_destino_databricks(
    destino: Path,
    landing_schema: str,
    volume_name: str,
):
    """Confere o destino com os recursos configurados pelo Bundle."""
    partes = destino.parts
    if len(partes) < 5 or partes[1] != "Volumes":
        raise ValueError(
            "destino local não permitido; informe um caminho /Volumes/..."
        )
    schema = partes[3]
    volume = partes[4]
    if schema != landing_schema:
        raise ValueError(
            f"schema Landing inválido: {schema!r}; "
            f"esperado: {landing_schema!r}"
        )
    if volume != volume_name:
        raise ValueError(
            f"Volume Landing inválido: {volume!r}; "
            f"esperado: {volume_name!r}"
        )


# GERADOR DE MESES (trata virada de ano)
def meses_no_intervalo(inicio: str, fim: str):
    ano_i, mes_i = map(int, inicio.split("-"))
    ano_f, mes_f = map(int, fim.split("-"))
    atual = date(ano_i, mes_i, 1)
    limite = date(ano_f, mes_f, 1)
    if atual > limite:
        raise ValueError("INICIO nao pode ser posterior a FIM")
    while atual <= limite:
        yield f"{atual.year:04d}-{atual.month:02d}"
        # avanca um mes
        if atual.month == 12:
            atual = date(atual.year + 1, 1, 1)
        else:
            atual = date(atual.year, atual.month + 1, 1)


def mes_anterior(referencia: date | None = None) -> str:
    """Retorna o mês anterior a uma data no formato AAAA-MM."""
    referencia = referencia or date.today()
    if referencia.month == 1:
        return f"{referencia.year - 1:04d}-12"
    return f"{referencia.year:04d}-{referencia.month - 1:02d}"


def ausencia_recente_permitida(ano_mes: str, fim: str) -> bool:
    """Permite atraso de publicação apenas na cauda mais recente do intervalo."""
    ano, mes = map(int, ano_mes.split("-"))
    ano_fim, mes_fim = map(int, fim.split("-"))
    diferenca = (ano_fim * 12 + mes_fim) - (ano * 12 + mes)
    return 0 <= diferenca < MESES_RECENTES_PERMITIDOS


# DOWNLOAD DE UM ARQUIVO (com retry e validacao de tamanho)
def parquet_integro(caminho: Path) -> bool:
    """Valida os marcadores de início e fim de um arquivo Parquet."""
    if not caminho.exists() or caminho.stat().st_size < 8:
        return False
    with caminho.open("rb") as arquivo:
        inicio = arquivo.read(4)
        arquivo.seek(-4, 2)
        fim = arquivo.read(4)
    return inicio == b"PAR1" and fim == b"PAR1"


def baixar(url: str, destino: Path, ausencia_esperada: bool = False) -> bool:
    if destino.suffix == ".parquet" and parquet_integro(destino):
        print(f"  ja existe e integro, pulando: {destino.name}")
        return True
    if destino.suffix != ".parquet" and destino.exists() and destino.stat().st_size > 0:
        print(f"  ja existe, pulando: {destino.name}")
        return True

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            with requests.get(
                url, headers=HTTP_HEADERS, stream=True, timeout=TIMEOUT
            ) as r:
                if r.status_code in (403, 404) and ausencia_esperada:
                    print(
                        f"  arquivo recente ainda nao publicado "
                        f"({r.status_code}), pulando: {destino.name}"
                    )
                    return True
                r.raise_for_status()
                tamanho_esperado = int(r.headers.get("Content-Length", 0))
                tmp = destino.with_suffix(destino.suffix + ".part")
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                # valida antes de promover o arquivo
                if tamanho_esperado and tmp.stat().st_size != tamanho_esperado:
                    tamanho_obtido = tmp.stat().st_size
                    tmp.unlink(missing_ok=True)
                    raise IOError(
                        f"tamanho divergente: {tamanho_obtido} vs {tamanho_esperado}"
                    )
                tmp.replace(destino)
            print(f"  ok ({destino.stat().st_size / 1e6:.1f} MB): {destino.name}")
            time.sleep(INTERVALO_REQUISICOES)
            return True
        except (requests.RequestException, IOError) as e:
            print(f"  tentativa {tentativa}/{MAX_TENTATIVAS} falhou: {e}")
            if tentativa < MAX_TENTATIVAS:
                print(
                    f"  aguardando {ESPERA_ENTRE_TENTATIVAS}s "
                    "antes da próxima tentativa"
                )
                time.sleep(ESPERA_ENTRE_TENTATIVAS)

    print(f"  FALHOU apos {MAX_TENTATIVAS} tentativas: {url}")
    return False


# EXECUCAO
def main():
    args = ler_argumentos()
    validar_destino_databricks(
        args.destino,
        args.landing_schema,
        args.volume_name,
    )
    args.destino.mkdir(parents=True, exist_ok=True)
    fim = mes_anterior() if args.fim.lower() == "auto" else args.fim
    falhas = []
    total = 0

    if BAIXAR_METADADOS:
        pasta_metadata = args.destino / "_metadata"
        pasta_metadata.mkdir(parents=True, exist_ok=True)
        print("\n=== Metadados oficiais ===")
        for nome, url in ARTEFATOS_METADATA:
            print(f"{nome}: {url}")
            if not baixar(url, pasta_metadata / nome):
                falhas.append(f"metadado {nome}")

    for tipo in TIPOS:
        print(f"\n=== Categoria: {tipo} ===")
        for ano_mes in meses_no_intervalo(args.inicio, fim):
            ano, mes = ano_mes.split("-")
            nome = f"{tipo}_tripdata_{ano_mes}.parquet"
            url = f"{BASE_URL}/{nome}"
            destino = args.destino / tipo / ano / mes / nome
            destino.parent.mkdir(parents=True, exist_ok=True)
            total += 1
            print(f"{tipo} {ano_mes}: {url}")
            if not baixar(
                url,
                destino,
                ausencia_esperada=ausencia_recente_permitida(ano_mes, fim),
            ):
                falha = f"{tipo} {ano_mes}"
                falhas.append(falha)
                raise RuntimeError(
                    f"extração interrompida no primeiro arquivo histórico "
                    f"ausente: {falha}; execute novamente para retomar"
                )

    print("\nResumo")
    print(f"  categorias: {', '.join(TIPOS)}")
    print(
        f"  metadados: "
        f"{len(ARTEFATOS_METADATA) if BAIXAR_METADADOS else 0} artefatos"
    )
    print(f"  intervalo: {args.inicio} a {fim} | arquivos tentados: {total}")
    print(f"  destino: {args.destino}")
    if falhas:
        print(f"  falhas ({len(falhas)}): {', '.join(falhas)}")
        raise RuntimeError(
            f"extração incompleta: {len(falhas)} arquivo(s) histórico(s) ausente(s)"
        )
    else:
        print("  tudo baixado com sucesso")


if __name__ == "__main__":
    main()
