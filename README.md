# Case iFood — pipeline Medallion no Databricks

O projeto baixa os arquivos de viagens da NYC TLC para um Unity Catalog
Volume e os processa em tabelas Delta com Lakeflow Spark Declarative
Pipelines.

## Estrutura

- `src/01_extracao.py`: baixa os Parquets e metadados oficiais para a Landing.
- `src/02_bronze.py`: cria as tabelas Bronze com Auto Loader e PySpark.
- `src/03_silver.py`: padroniza as viagens de janeiro a maio de 2023 na Silver.
- `src/04_gold.py`: publica as duas tabelas Gold governadas para consumo.
- `src/metadata/`: extrai deterministicamente os contratos oficiais.
- `src/quality/`: contém o catálogo e a avaliação das regras de Data Quality.
- `src/databricks/resources/`: define Jobs, Pipeline, schemas, Volume e dashboard.
- `src/databricks/dashboards/`: mantém o dashboard Databricks AI/BI como código.
- `src/databricks/databricks.yml`: configura o Bundle e os ambientes.
- `analysis/perguntas/`: contém o notebook SQL com as respostas do case.
- `analysis/eda/`: contém o notebook SQL de análise exploratória da Gold.

## Modelo de execução

O processamento é executado integralmente no Databricks. O repositório local
é usado somente para versionar o código, validar o Bundle e realizar o deploy.
O script de extração exige como destino um Unity Catalog Volume e rejeita
diretórios locais.

O `requirements.txt` descreve o ambiente virtual local para desenvolvimento e
validações auxiliares. Os workloads produtivos não dependem desse ambiente:
suas bibliotecas são declaradas nos `environment` dos Jobs serverless do
Bundle.

## Autenticação no Databricks Free

Use um perfil separado para impedir que o projeto seja implantado em outro
workspace. Substitua a URL pela URL exata do workspace Free:

```bash
databricks auth login \
  --host https://dbc-fcfa10b2-faab.cloud.databricks.com \
  --profile ap_ifood
```

Antes da primeira implantação, o catálogo `case_ifood` deve existir no Unity
Catalog. No Databricks Free, crie-o pela interface como um catálogo `Standard`
usando `Default Storage`. O Bundle cria os schemas e o Volume dentro desse
catálogo, mas não cria o catálogo.

Confirme a identidade e o host antes de qualquer operação:

```bash
databricks auth describe --profile ap_ifood
```

## Recursos implantados

O host do workspace Free e o perfil `ap_ifood` estão fixados no target
`free`, que é o único target e também o padrão do Bundle. Os schemas seguem
o padrão `tlc_data_<layer>`:

- `case_ifood.tlc_data_landing`;
- `case_ifood.tlc_data_bronze`;
- `case_ifood.tlc_data_silver`;
- `case_ifood.tlc_data_gold`;
- `case_ifood.tlc_data_quality`.

O Volume gerenciado de arquivos originais é
`case_ifood.tlc_data_landing.nyc_tlc_landing`.

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

## Data Quality

O contrato versionado `src/quality/rules.yml` possui nove regras, avaliadas nas
camadas Landing, Bronze, Silver e Gold:

- completude de `VendorID`, `passenger_count` e `total_amount`;
- quantidade de passageiros não positiva;
- valor total não positivo;
- desembarque anterior ao embarque;
- duração superior a 24 horas;
- embarque anterior ao mês de referência;
- embarque ou desembarque posterior ao mês de referência.

`quality_setup` materializa o catálogo de regras, a tabela histórica de
resultados e as views de monitoramento em `tlc_data_quality`.
`quality_monitoring` avalia as regras depois de cada atualização bem-sucedida
da Medallion. Se houver mais de uma avaliação no mesmo dia, apenas a execução
mais recente daquele dia permanece no histórico.

As regras sinalizam violações, mas não removem registros automaticamente. A
exclusão acontece somente quando o contrato de um produto de consumo a declara,
como em `gold_taxi_passengers_by_hour`, que calcula a média apenas com
`passenger_count > 0`.

## Dashboard

O dashboard AI/BI `dashboard_monitoramento_qualidade_nyc_tlc` é definido em
`src/databricks/dashboards/data_quality.lvdash.json` e implantado pelo Bundle.
Ele usa o SQL Warehouse configurado em `sql_warehouse_id` e reúne três abas:

- `Monitoramento de qualidade`;
- `Metadados`;
- `Profiling`.

Alterações feitas diretamente na GUI devem ser exportadas e sincronizadas com
o arquivo `.lvdash.json` antes de um novo deploy, para que o dashboard remoto e
o repositório continuem equivalentes.

## EDA

A análise exploratória foi conduzida de forma incremental no Databricks, sobre
os dados publicados pela arquitetura Medallion. A finalidade não foi corrigir
silenciosamente os dados originais, mas entender sua estrutura, distinguir
problemas de transformação de características da fonte e converter os achados
em decisões reproduzíveis de modelagem, observabilidade e qualidade.

### Escopo e abordagem

A exploração partiu da Gold
`case_ifood.tlc_data_gold.gold_yellow_trips_consumption`, que contém os cinco
atributos obrigatórios do case, e foi ampliada para
`gold_taxi_passengers_by_hour` quando a semântica da segunda pergunta exigiu
considerar Yellow e Green Taxi. A janela analítica é de janeiro a maio de 2023;
para a média de passageiros por hora, o mês de referência é maio de 2023.

O processo adotado foi:

1. definir o escopo e compreender os dados, seus contratos e a janela temporal;
2. realizar análises estruturais, estatísticas e temporais;
3. identificar e investigar anomalias e problemas de completude ou validade;
4. validar as hipóteses entre as camadas e contra os metadados oficiais;
5. aplicar os achados na modelagem, no pipeline e nas regras de Data Quality;
6. documentar e monitorar continuamente os resultados.

A execução visual está centralizada nas abas `Profiling`, `Metadados` e
`Monitoramento de qualidade` do dashboard principal. O profiling e a detecção
de anomalias nativos do Databricks complementam as consultas SQL versionadas no
dashboard. As respostas do desafio permanecem versionadas em
`analysis/perguntas/01_respostas_analiticas.sql`.

EDA e Data Quality têm papéis diferentes neste projeto. A EDA formula e testa
hipóteses; o catálogo `src/quality/rules.yml` transforma os achados que precisam
de acompanhamento contínuo em regras executáveis. As violações são
monitoradas, e não removidas automaticamente, salvo quando uma regra explícita
do produto Gold determina a exclusão.

### Hipóteses avaliadas e consequências

| Hipótese investigada | Evidência e conclusão | Consequência no projeto |
|---|---|---|
| `VendorID` e `passenger_count` totalmente nulos indicavam ausência desses campos nos arquivos oficiais. | O primeiro profiling apresentou ambos como 100% nulos. A inspeção entre camadas mostrou que valores declarados com variação de schema físico podiam estar em `_rescued_data`. Após a recuperação canônica, `VendorID` passou a ser preenchido; parte de `passenger_count` continuou nula na própria origem. A hipótese foi rejeitada para `VendorID` e parcialmente confirmada para `passenger_count`. | A Bronze passou a recuperar e converter deterministicamente valores de `_rescued_data`. Foram criadas regras de completude para `VendorID`, `passenger_count` e `total_amount` em todas as camadas. Nulos legítimos de `passenger_count` são preservados e monitorados. |
| Datas de viagem em 2001 significavam que a Silver havia processado arquivos fora da janela de janeiro a maio de 2023. | A Silver limita arquivos por `_reference_month`, extraído do nome do Parquet. Um arquivo de 2023 pode conter timestamps de viagem fora do seu mês de referência; portanto, não houve vazamento de partições antigas para a Silver. A errata apresentada, referente a arquivos de 2015 a 2017, não explica registros dos arquivos de 2023. | `_reference_month` foi preservado até a Gold para rastreabilidade. Foram criadas as regras `pickup_before_reference_month` e `trip_datetime_after_reference_month`, aplicadas da Landing à Gold. Os registros permanecem disponíveis para auditoria em vez de serem descartados silenciosamente. |
| Todo `passenger_count` nulo ou igual a zero deveria ser convertido para zero ou removido do pipeline. | Nulo significa quantidade não informada e não é semanticamente equivalente a zero. Valores não positivos também não representam uma quantidade válida para calcular a média solicitada, mas devem continuar rastreáveis. | Os dados detalhados são preservados. A regra `passenger_count_null` mede completude e `passenger_count_non_positive` mede validade. Apenas `gold_taxi_passengers_by_hour` exclui nulos e valores menores ou iguais a zero do denominador, expondo também `trips_discarded`. |
| `total_amount <= 0` era necessariamente erro técnico e deveria ser eliminado. | Valores negativos ou iguais a zero podem representar ajustes, estornos ou registros operacionais da fonte. Não foi encontrada evidência suficiente para reescrevê-los. | A consulta mensal preserva esses valores e documenta a premissa. `total_amount_null` e `total_amount_non_positive` monitoram completude e validade sem alterar o dado original. |
| Desembarque anterior ao embarque ou duração superior a 24 horas poderia ser uma regra normal da operação. | Os metadados definem os campos como início e fim da mesma viagem e não oferecem justificativa semântica para essas ocorrências. Elas foram tratadas como anomalias observáveis, não como motivo automático de exclusão. | Foram criadas as regras `invalid_trip_chronology` e `trip_duration_over_24_hours` em Landing, Bronze, Silver e Gold. |
| A Gold obrigatória deveria combinar todos os datasets TLC. | Os nomes exigidos `tpep_pickup_datetime` e `tpep_dropoff_datetime` pertencem ao contrato Yellow. Green usa campos `lpep`, e FHV/HVFHV possuem outra semântica e outro conjunto de atributos. | `gold_yellow_trips_consumption` permanece Yellow-only e atende literalmente ao contrato de consumo, sem renomear campos de serviços diferentes para forçar compatibilidade. |
| “Todos os táxis da frota” na pergunta de passageiros significava somente Yellow Taxi. | A redação da primeira pergunta especifica Yellow, enquanto a segunda diz todos os táxis. No contexto regulatório da NYC TLC, Yellow e Green são táxis; FHV e High Volume FHV são veículos for-hire e não fornecem `passenger_count`. | Foi criada `gold_taxi_passengers_by_hour`, unindo Yellow e Green com harmonização dos timestamps `tpep` e `lpep`. A tabela expõe contagens por serviço, serviços participantes, arquivos de origem e ingestão mais recente. |
| A ausência dos meses mais recentes indicava necessariamente falha do extrator. | A publicação dos arquivos TLC possui atraso e categorias diferentes podem ficar disponíveis em momentos distintos. Entretanto, lacunas em 2024 ou 2025 não poderiam ser explicadas por esse atraso recente. | O extrator percorre inclusivamente todos os meses entre `inicio` e `fim`, atravessa viradas de ano, ignora arquivos já íntegros, interrompe diante de lacuna histórica e tolera `403/404` apenas na janela recente. Com `fim=auto`, tenta até o mês anterior. |
| Descrições traduzidas ou produzidas manualmente seriam equivalentes ao contrato oficial. | Tradução e redação manual introduziam interpretação não rastreável. O slug `hvfhs` do PDF também difere do slug `fhvhv` dos dados mensais. | Os contratos são extraídos deterministicamente dos PDFs em inglês, com checksum SHA-256. Bronze, Silver e Gold herdam essas descrições; o de-para `hvfhs -> fhvhv` é explícito. |

### Limites conhecidos

- O profiling detalhado do dashboard concentra-se na Gold Yellow; a segunda
  Gold é um agregado especializado, cuja composição é observada pelas métricas
  de viagens consideradas, descartadas e separadas por serviço.
- As estatísticas descritivas mostram associação e distribuição, mas não
  demonstram causalidade.
- Regras com severidade de monitoramento não constituem, por si só, autorização
  para excluir ou corrigir registros.
- A Landing preserva os Parquets oficiais. Bronze normaliza schema e acrescenta
  rastreabilidade; Silver padroniza e limita a janela; Gold aplica o contrato
  específico de consumo.
- Novas janelas, atributos ou hipóteses devem ser incorporados primeiro como
  configuração ou regra versionada e depois reavaliados no profiling, evitando
  ajustes manuais sem rastreabilidade.

## Primeira implantação

O deploy cria ou atualiza os recursos, mas não executa os Jobs. Na primeira
implantação, materialize o contrato de qualidade antes de executar a ingestão:

```bash
cd src/databricks
databricks bundle validate --profile ap_ifood
databricks bundle deploy --profile ap_ifood
databricks bundle run quality_setup --profile ap_ifood
databricks bundle run ingestion \
  --params inicio=2023-01,fim=auto \
  --profile ap_ifood
```

O Job `ingestion` encadeia:

1. extração dos Parquets e metadados para a Landing;
2. geração determinística dos contratos;
3. atualização da Pipeline Medallion, da Bronze até a Gold;
4. execução do Job de Data Quality.

O usuário ou service principal do deploy precisa ter permissão para criar
schemas e volumes no catálogo existente e para usar o SQL Warehouse do
dashboard.

## Atualização recorrente

O Job `job_ingestao_nyc_tlc` está agendado para o dia 15 de cada mês, às 08h,
no fuso `America/Sao_Paulo`. Seus parâmetros padrão percorrem desde `2023-01`
até o mês anterior (`fim=auto`). Arquivos íntegros já existentes são ignorados,
portanto a execução é retomável e preenche novas disponibilidades sem baixar
novamente todo o histórico.

Para publicar uma mudança de código ou configuração:

```bash
cd src/databricks
databricks bundle validate --profile ap_ifood
databricks bundle deploy --profile ap_ifood
databricks bundle run ingestion \
  --params inicio=2023-01,fim=auto \
  --profile ap_ifood
```

Para avaliar novamente a qualidade sem refazer a carga:

```bash
cd src/databricks
databricks bundle run quality_monitoring --profile ap_ifood
```

Uma atualização completa da Pipeline deve ser usada apenas quando houver
mudança de lógica, schema ou necessidade explícita de recomputar as tabelas:

```bash
cd src/databricks
databricks bundle run medallion \
  --full-refresh-all \
  --profile ap_ifood
```
