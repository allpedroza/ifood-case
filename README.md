# Case iFood — pipeline Medallion no Databricks

O projeto baixa os arquivos de viagens da NYC TLC para um Unity Catalog
Volume e os processa em tabelas Delta com Lakeflow Spark Declarative
Pipelines.

## Estrutura

- `src/01_extracao.py`: baixa os Parquets e metadados oficiais para a Landing.
- `src/02_bronze.py`: cria as tabelas Bronze com Auto Loader e PySpark.
- `src/03_silver.py`: padroniza as viagens de janeiro a maio de 2023 na Silver.
- `src/04_gold.py`: publica a tabela Yellow governada para consumo.
- `src/databricks/resources/`: define o Job, a Pipeline, os schemas e o Volume.
- `src/databricks/databricks.yml`: configura o Bundle e os ambientes.
- `analysis/`: contém scripts e notebooks com as respostas do case.

## Modelo de execução

O processamento é executado integralmente no Databricks. O repositório local
é usado somente para versionar o código, validar o Bundle e realizar o deploy.
O script de extração exige como destino um Unity Catalog Volume e rejeita
diretórios locais.

## Autenticação no Databricks Free

Use um perfil separado para impedir que o projeto seja implantado em outro
workspace. Substitua a URL pela URL exata do workspace Free:

```bash
databricks auth login \
  --host https://dbc-fcfa10b2-faab.cloud.databricks.com \
  --profile ap_ifood
```

## Validar e implantar

O host do workspace Free e o perfil `ap_ifood` estão fixados no target
`free`, que é o único target e também o padrão do Bundle. Os schemas seguem
o padrão `tlc_data_<layer>`: `tlc_data_landing` e `tlc_data_bronze`.
O Volume de arquivos originais é `nyc_tlc_landing`.

Antes dos dados mensais, a extração baixa uma única vez oito artefatos
oficiais para `tlc/_metadata`: guia de uso, quatro dicionários, nota do
formato Parquet, lookup de zonas e shapefile. A flag `BAIXAR_METADADOS`
controla esse comportamento. Na Bronze, `bronze_metadata_artifacts`
mantém o inventário e a rastreabilidade dos arquivos, enquanto
`bronze_taxi_zone_lookup` expõe a lookup geográfica. O de-para entre o
dicionário `hvfhs` e o dataset `fhvhv` é registrado explicitamente.
O Job mensal sempre percorre de `2023-01` até o mês anterior (`fim=auto`);
arquivos já íntegros são ignorados, e lacunas históricas são preenchidas.

`src/metadata/generate_contracts.py` lê deterministicamente os PDFs oficiais
em inglês, registra checksum SHA-256 e gera contratos YAML no Volume em
`tlc/_metadata/contracts`. Esses contratos são a única fonte de descrições
para Bronze, Silver e Gold. A pipeline também publica
`bronze_metadata_columns`, onde `status` identifica colunas `DOCUMENTED`,
`UNDOCUMENTED` ou `MISSING_IN_SOURCE`.
Os schemas e comentários são declarados diretamente pela pipeline; não há
uma etapa posterior para alterar metadados.

A pipeline Silver publica quatro tabelas de viagens limitadas a `2023-01`
até `2023-05` e uma lookup geográfica atemporal. Nomes e tipos são
padronizados, e os comentários de coluna são herdados dos mesmos contratos
usados pela Bronze.

A Gold publica `gold_yellow_trips_consumption` com os campos obrigatórios
`VendorID`, `passenger_count`, `total_amount`, `tpep_pickup_datetime` e
`tpep_dropoff_datetime`, preservando as descrições em inglês extraídas
deterministicamente do PDF oficial.

A Gold também publica `gold_taxi_passengers_by_hour`, que interpreta táxi no
sentido regulatório e combina, por `UNION ALL`, viagens Yellow e Green de maio
de 2023. FHV e High Volume FHV ficam fora desse produto. Contagens nulas ou
menores ou iguais a zero são excluídas da média e contabilizadas separadamente.

```bash
cd src/databricks
databricks bundle validate
databricks bundle deploy
databricks bundle run ingestion \
  -- --inicio=2023-01 --fim=2023-05
```

Antes do deploy, confirme a identidade e o host:

```bash
databricks auth describe --profile ap_ifood
```

O usuário ou service principal usado no deploy precisa ter permissão para
criar schemas e volumes no catálogo selecionado.
