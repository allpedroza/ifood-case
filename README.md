# Case iFood: Data Governance Engineer (TLC Trip Record Data)

O projeto baixa os arquivos de viagens da NYC TLC para um Unity Catalog
Volume e os processa em tabelas Delta com Lakeflow Spark Declarative
Pipelines.

## Entrega do case

As duas respostas pedidas estão nestes arquivos:

- [SQL executável](analysis/perguntas/01_respostas_analiticas.sql);
- [respostas e premissas](analysis/perguntas/01_respostas_analiticas.md);
- [média mensal de total_amount em CSV](analysis/perguntas/01_media_total_mensal.csv);
- [média de passageiros por hora em CSV](analysis/perguntas/02_media_passageiros_hora_maio.csv).

O primeiro resultado consulta
`case_ifood.tlc_data_gold.gold_yellow_trips_consumption`. O segundo usa
`case_ifood.tlc_data_gold.gold_taxi_passengers_by_hour`, que reúne Yellow e
Green Taxi em maio de 2023. A seção de EDA e os componentes de governança
documentam as decisões que levaram a essas tabelas, mas não são necessários
para localizar as respostas.

## Estrutura

- `src/01_extracao.py`: valida a Landing e baixa fontes quando há acesso externo.
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
- `deploy_databricks.sh`: conduz o deploy interativo a partir do terminal.
- `scripts/stage_landing.sh`: transfere as fontes oficiais para o Volume.

## Modelo de execução

Transformações, contratos, tabelas e regras de qualidade são executados no
Databricks. Na Free Edition, o cliente local também transporta os arquivos
oficiais até o Unity Catalog Volume porque o compute pode bloquear os domínios
de origem. Nenhuma transformação de dados ocorre na máquina local.

O repositório local é usado para versionar o código, validar e implantar o
Bundle e preparar a Landing com `scripts/stage_landing.sh`. O script executado
no Job exige como destino um Unity Catalog Volume e rejeita diretórios locais.

O `requirements.txt` descreve o ambiente virtual local para desenvolvimento e
validações auxiliares. Os workloads produtivos não dependem desse ambiente:
suas bibliotecas são declaradas nos `environment` dos Jobs serverless do
Bundle.

## Ambiente suportado

O runtime suportado é o Databricks Free atual, com Unity Catalog, Volumes,
SQL Warehouse, Jobs serverless e Lakeflow Spark Declarative Pipelines. Os
scripts de transformação importam `pyspark.pipelines`, e a Pipeline usa
`serverless: true` com o canal `CURRENT`.

O projeto não oferece execução equivalente em Spark local ou no Community
Edition clássico. Nesses ambientes faltam os serviços gerenciados usados pelo
Bundle. O `requirements.txt` permite editar, inspecionar Parquets e validar
partes isoladas do código; ele não substitui o runtime do Databricks.

## Como configurar e executar no Databricks Free

Este roteiro parte de uma conta Databricks Free sem recursos do projeto. O
Bundle não fixa workspace, profile ou SQL Warehouse, por isso os mesmos
arquivos podem ser implantados em outra conta.

### Caminho rápido com o assistente

Com `git` e Databricks CLI instalados, crie a conta, o catálogo `case_ifood` e
confirme que há um SQL Warehouse. As instruções completas estão no caminho
manual logo abaixo. Depois, execute na raiz do repositório:

```bash
chmod +x deploy_databricks.sh
./deploy_databricks.sh
```

O assistente pede o host do workspace, o nome do profile, o catálogo, o ID do
SQL Warehouse e a janela da carga. Em seguida, ele:

1. autentica a CLI por OAuth;
2. confirma se o catálogo e o warehouse existem;
3. executa `bundle validate` e `bundle deploy`;
4. baixa os arquivos oficiais localmente e os envia ao Volume;
5. configura Data Quality e executa a primeira carga;
6. mostra os recursos implantados.

Credenciais e parâmetros pessoais não são gravados no repositório. O profile
OAuth fica no arquivo de configuração local da Databricks CLI. Cada arquivo é
validado e removido da pasta temporária após o upload, sem manter uma cópia
completa da Landing na máquina.

<details>
<summary>Caminho manual, começando do zero</summary>

### 1. Instalar as ferramentas locais

O deploy é iniciado no terminal da máquina do usuário. Instale primeiro:

- `git`, para clonar o repositório;
- Databricks CLI versão 0.205 ou superior.

No macOS, instale a CLI com Homebrew:

```bash
brew install databricks/tap/databricks
```

No Windows, use WinGet:

```powershell
winget install Databricks.DatabricksCLI
```

No Linux, use o instalador oficial:

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

Confirme as duas instalações antes de continuar:

```bash
git --version
databricks version
```

A versão exibida pela Databricks CLI deve ser 0.205 ou superior. Consulte as
[instruções oficiais de instalação](https://docs.databricks.com/aws/pt/dev-tools/cli/install)
se o comando não for encontrado.

### 2. Criar a conta Databricks Free

Crie uma conta na
[Databricks Free Edition](https://docs.databricks.com/aws/pt/getting-started/free-edition).
Ao finalizar o cadastro, o Databricks cria um workspace com compute serverless
e armazenamento padrão. Entre nesse workspace antes de continuar.

### 3. Identificar o host e o workspace ID

Observe a URL aberta no navegador. Ela segue este formato:

```text
https://dbc-xxxxxxxx-xxxx.cloud.databricks.com/browse?o=123456789
```

Separe os dois valores:

- `<WORKSPACE_URL>` é somente
  `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`;
- `<WORKSPACE_ID>` é o número após `o=`.

O login da CLI usa `<WORKSPACE_URL>`. O Bundle não exige nem armazena
`<WORKSPACE_ID>`; esse número é útil para reconhecer links da interface. Não
copie `/browse`, `?o=` ou qualquer outro caminho para o parâmetro `--host`.

### 4. Criar o catálogo

Na interface do workspace:

1. acesse `Catalog`;
2. selecione `Create catalog`;
3. informe `case_ifood`;
4. escolha o tipo `Standard`;
5. mantenha `Default Storage`;
6. conclua a criação.

O catálogo precisa existir antes do deploy. O Bundle cria os schemas, o Volume
e os demais recursos dentro dele. Uma pasta chamada `case_ifood` em
`Workspace` não substitui o catálogo do Unity Catalog.

### 5. Identificar o SQL Warehouse

Abra `SQL Warehouses` e confirme que existe um warehouse serverless, como o
`Serverless Starter Warehouse`. Por enquanto, localize o warehouse pela
interface. O ID será consultado pela CLI após a autenticação. O repositório
não fornece um valor padrão porque esse identificador pertence ao workspace
de destino.

### 6. Clonar o repositório e autenticar a CLI

Em um diretório de trabalho:

```bash
git clone https://github.com/allpedroza/ifood-case.git
cd ifood-case

databricks auth login \
  --host <WORKSPACE_URL> \
  --profile <PROFILE>
```

O navegador abrirá o login OAuth. Entre com a mesma conta usada para criar o
workspace e autorize a CLI. `<PROFILE>` é um nome local escolhido pelo usuário
para identificar essa conexão.

Confirme a identidade, o host e o catálogo antes de criar recursos:

```bash
databricks auth describe --profile <PROFILE>
databricks current-user me --profile <PROFILE>
databricks catalogs get case_ifood --profile <PROFILE>
```

Liste os warehouses e copie o campo `id` do warehouse escolhido:

```bash
databricks warehouses list --profile <PROFILE>
```

Esse valor será usado como `<WAREHOUSE_ID>` nos comandos seguintes.

### 7. Validar e implantar o Bundle

Execute os comandos a partir da pasta que contém `databricks.yml`:

```bash
cd src/databricks

databricks bundle validate \
  -t free \
  --profile <PROFILE> \
  --var="sql_warehouse_id=<WAREHOUSE_ID>"

databricks bundle deploy \
  -t free \
  --profile <PROFILE> \
  --var="sql_warehouse_id=<WAREHOUSE_ID>"
```

O deploy cria cinco schemas, um Volume, três Jobs, uma Pipeline Lakeflow e um
dashboard. Ele não inicia a ingestão e mantém o agendamento mensal pausado.

### 8. Executar a primeira carga

Na Free Edition, prepare a Landing pelo cliente local porque o acesso de saída
do compute serverless é restrito. A carga padrão do case vai de janeiro até
maio de 2023:

```bash
cd ../..
scripts/stage_landing.sh \
  --profile <PROFILE> \
  --catalog case_ifood \
  --inicio 2023-01 \
  --fim 2023-05
cd src/databricks
```

Depois materialize o catálogo de regras e execute o Job:

```bash
databricks bundle run quality_setup \
  -t free \
  --profile <PROFILE> \
  --var="sql_warehouse_id=<WAREHOUSE_ID>"

databricks bundle run ingestion \
  -t free \
  --params inicio=2023-01,fim=2023-05 \
  --profile <PROFILE> \
  --var="sql_warehouse_id=<WAREHOUSE_ID>"
```

O Job confirma a Landing, gera os contratos e atualiza Bronze e Silver. Em
seguida, executa o gate de qualidade e, se não houver `FAIL`, atualiza a Gold.
O mesmo pipeline Medallion recebe duas atualizações seletivas e sequenciais.
Isso respeita o limite da Free Edition e evita um segundo pipeline declarativo.
Não inicie `medallion` em paralelo, pois um pipeline aceita somente um update
ativo.

### 9. Conferir a implantação

Liste os recursos gerenciados pelo Bundle:

```bash
databricks bundle summary \
  -t free \
  --profile <PROFILE> \
  --var="sql_warehouse_id=<WAREHOUSE_ID>"
```

Na interface, acesse `Catalog > case_ifood`. Ao final da primeira carga devem
existir:

- `tlc_data_landing`, com o Volume `nyc_tlc_landing`;
- `tlc_data_bronze`, com as quatro tabelas de viagens e as tabelas de
  metadados;
- `tlc_data_silver`, com as viagens padronizadas;
- `tlc_data_gold`, com as duas tabelas de consumo;
- `tlc_data_quality`, com regras, resultados e views de monitoramento.

Uma conferência rápida pode ser feita no SQL Editor:

```sql
SELECT
  MIN(_reference_month) AS mes_inicial,
  MAX(_reference_month) AS mes_final,
  COUNT(*) AS registros
FROM case_ifood.tlc_data_gold.gold_yellow_trips_consumption;
```

O resultado esperado cobre `2023-01` até `2023-05`. As contagens podem mudar
se a NYC TLC republicar os arquivos.

</details>

### Acesso externo na Free Edition

O
[acesso de saída da Free Edition é restrito](https://docs.databricks.com/aws/pt/getting-started/free-edition-limitations)
a alguns domínios confiáveis. Por isso o caminho recomendado usa
`scripts/stage_landing.sh`: o cliente local acessa `home4.nyc.gov` e
`d37ci6vzurychx.cloudfront.net`, valida os arquivos e os envia ao Unity Catalog
Volume pela CLI.

Se a conta tiver acesso externo liberado, o Job também consegue baixar
arquivos ausentes diretamente. Em caso de falha DNS, o extrator interrompe na
primeira tentativa e orienta o uso do staging local, em vez de aguardar todos
os ciclos de retry.

## Recursos implantados

O target `free` usa `mode: production` para manter os nomes físicos sem
prefixos de desenvolvimento. O profile é informado pela CLI. O ID do SQL
Warehouse também é obrigatório no deploy, por meio da variável
`sql_warehouse_id`.

Os schemas seguem o padrão `tlc_data_<layer>`:

- `case_ifood.tlc_data_landing`;
- `case_ifood.tlc_data_bronze`;
- `case_ifood.tlc_data_silver`;
- `case_ifood.tlc_data_gold`;
- `case_ifood.tlc_data_quality`.

O Volume gerenciado de arquivos originais é
`case_ifood.tlc_data_landing.nyc_tlc_landing`.

Antes dos dados mensais, o staging transfere uma única vez oito artefatos
oficiais para `tlc/_metadata`: guia de uso, quatro dicionários, nota do formato
Parquet, lookup de zonas e shapefile. Em ambientes com acesso externo, a flag
`BAIXAR_METADADOS` permite que o próprio Job faça esse download. Na Bronze,
`bronze_metadata_artifacts` mantém o inventário e a rastreabilidade dos
arquivos, enquanto `bronze_taxi_zone_lookup` expõe a lookup geográfica. O
de-para entre o dicionário `hvfhs` e o dataset `fhvhv` é registrado
explicitamente.
O Job usa `2023-01` a `2023-05` como padrão, que é o período do case. O
extrator aceita outros meses ou `fim=auto`. Arquivos já íntegros são ignorados,
e lacunas históricas são preenchidas.

### Republicação dos arquivos

O nome e o caminho de cada Parquet são determinísticos. O staging local baixa
um arquivo por vez em uma pasta temporária, valida os marcadores Parquet,
envia ao Volume e remove a cópia local. O extrator do Job também valida
arquivos existentes e usa `.part` quando realiza um download direto. Em novas
execuções, um arquivo presente no mesmo caminho é ignorado. Essa é a
idempotência usada na Landing.

A NYC TLC pode republicar um mês. O fluxo não substitui automaticamente um
arquivo já íntegro, e os registros não possuem uma chave oficial de viagem que
permita deduplicação confiável por linha. Uma republicação exige tratamento
controlado do arquivo afetado e full refresh da Pipeline. Anexar a nova versão
à tabela existente sem recomputação pode produzir contagens duplicadas.

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
O Job executa uma atualização seletiva de Landing lógica, Bronze e Silver no
único pipeline Medallion. Em seguida, `quality_monitoring` avalia essas camadas
com o gate habilitado. Resultados `WARN` são registrados e permitem a promoção;
um resultado `FAIL` interrompe o Job antes da atualização seletiva das tabelas
Gold. O contrato deste case resolve todas as nove regras como `WARN`.

Depois da Gold, o Job avalia novamente as quatro camadas para consolidar o
monitoramento usado pelo dashboard. Se houver mais de uma avaliação no mesmo
dia, apenas a execução consolidada mais recente permanece no histórico.

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

### Correção da Bronze e estratégia de branches

A correção da Bronze é o ponto de separação entre as duas versões do projeto.
O primeiro profiling mostrou `VendorID` e `passenger_count` completamente
nulos. A comparação do arquivo de origem com as camadas mostrou uma diferença
entre a estrutura física dos Parquets e o schema do contrato. O Auto Loader
havia direcionado campos com variações de nome ou tipo para `_rescued_data`, e
a transformação não recuperava esses valores.

A Bronze passou a reconciliar as colunas do contrato com os valores resgatados,
aplicar o tipo canônico e manter os campos não reconhecidos para auditoria.
Depois do reprocessamento, `VendorID` deixou de apresentar a nulidade indevida.
Os nulos remanescentes de `passenger_count` estavam na origem e passaram a ser
monitorados por Data Quality.

As branches usam essa correção como ponto de separação:

- `original`, congelada no commit `c1faa2d`, representa o pipeline anterior à
  identificação e correção do gap estrutural dos Parquets;
- `main` contém a recuperação canônica introduzida pelo bugfix da Bronze a
  partir do commit `3fdb0c0` e toda a evolução posterior;
- `original` é uma referência histórica para comparação e não é a branch
  implantável;
- `main` é a fonte vigente para deploy no Databricks.

### Notas da investigação

Quando rodei o primeiro profiling, `VendorID` e `passenger_count` apareceram
100% nulos. Minha primeira suspeita foi a fonte, mas os Parquets ainda
continham parte desses valores. O problema estava na leitura: diferenças de
nome ou tipo levavam campos para `_rescued_data`, e a Bronze não os recuperava.
Esse achado deu origem ao bugfix que separa `original` de `main`. Depois do
reprocessamento, `VendorID` passou a ser preenchido. Os nulos que restaram em
`passenger_count` já estavam na origem, por isso entraram no monitoramento de
completude em vez de receber uma imputação.

As datas de 2001 levantaram outra suspeita. Parecia que a Silver havia deixado
entrar arquivos fora de janeiro a maio de 2023. A checagem de
`_reference_month` mostrou outra coisa: os arquivos eram de 2023, mas alguns
registros traziam timestamps antigos. A errata da TLC que consultei trata de
arquivos de 2015 a 2017 e não explica esse caso. Mantive os registros e levei
`_reference_month` até a Gold. As regras
`pickup_before_reference_month` e `trip_datetime_after_reference_month`
registram a divergência.

Também considerei o que fazer com `passenger_count` nulo ou igual a zero. Nulo
é ausência de informação; zero é um valor informado. Tratar os dois como a
mesma coisa esconderia um problema de completude. A tabela detalhada mantém os
dois casos. O agregado `gold_taxi_passengers_by_hour` usa apenas valores
positivos na média e informa quantas viagens foram descartadas.

Os valores não positivos de `total_amount` ficaram na base. Eles podem ser
ajustes ou estornos, e os documentos consultados não sustentam uma correção
automática. A EDA mostra a conta original e uma análise de sensibilidade sem
negativos. A regra `total_amount_non_positive` permite acompanhar os dois
cenários sem reescrever a fonte.

Para cronologia, adotei o mesmo cuidado. Desembarque anterior ao embarque e
duração acima de 24 horas não têm justificativa nos metadados, mas isso não
basta para apagar a viagem. As regras `invalid_trip_chronology` e
`trip_duration_over_24_hours` sinalizam esses registros nas quatro camadas.

A modelagem da Gold exigiu duas leituras do enunciado. A tabela obrigatória
continua restrita a Yellow porque os nomes `tpep_pickup_datetime` e
`tpep_dropoff_datetime` pertencem a esse contrato. Já a pergunta sobre "todos
os táxis" inclui Yellow e Green no sentido regulatório. FHV e High Volume FHV
ficam fora porque não são táxis e não fornecem `passenger_count`. Essa diferença
de escopo gerou `gold_taxi_passengers_by_hour`.

Nem toda ausência recente é falha de extração. A TLC publica categorias em
ritmos diferentes, então o extrator tolera `403` e `404` na cauda mais recente.
Esse atraso não explicava os buracos de 2024 e 2025 que apareceram durante os
testes. Por isso o loop percorre todos os meses entre `inicio` e `fim`, cruza a
virada de ano e para no primeiro arquivo histórico ausente.

Por fim, descartei a manutenção manual das descrições. Os contratos são gerados
dos PDFs em inglês, com checksum SHA-256. Isso evita que uma tradução passe a
parecer documentação oficial. O caso `hvfhs` no PDF e `fhvhv` nos arquivos
continua registrado como de-para explícito.

### O que ficou em aberto

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

O roteiro completo está em
[Como configurar e executar no Databricks Free](#como-configurar-e-executar-no-databricks-free).
O deploy cria ou atualiza os recursos, mas não executa os Jobs. Na primeira
implantação, `quality_setup` deve terminar antes de `ingestion`.

O assistente prepara a Landing antes de iniciar o Job. A partir daí,
`ingestion` encadeia:

1. validação dos Parquets e metadados presentes na Landing;
2. geração determinística dos contratos;
3. atualização da Pipeline Medallion, da Bronze até a Gold;
4. execução do Job de Data Quality.

O usuário ou service principal do deploy precisa ter permissão para criar
schemas e volumes no catálogo existente e para usar o SQL Warehouse do
dashboard.

## Atualização recorrente

O Job `job_ingestao_nyc_tlc` inclui um agendamento para o dia 15 de cada mês, às
08h, no fuso `America/Sao_Paulo`, mas é implantado como `PAUSED`. A execução
padrão termina em `2023-05`. Assim, um deploy de avaliação não inicia cargas
recorrentes nem baixa dados fora do período solicitado.

Na Free Edition sem acesso externo, mantenha o schedule pausado. Para ampliar
a janela, prepare primeiro os novos arquivos pela máquina local e depois
execute o Job. Arquivos já presentes no Volume são ignorados:

```bash
scripts/stage_landing.sh \
  --profile <PROFILE> \
  --catalog case_ifood \
  --inicio 2023-01 \
  --fim auto

cd src/databricks
databricks bundle run ingestion \
  -t free \
  --params inicio=2023-01,fim=auto \
  --profile <PROFILE> \
  --var="sql_warehouse_id=<WAREHOUSE_ID>"
```

Para publicar uma mudança de código ou configuração sem alterar a janela:

```bash
cd src/databricks
databricks bundle validate \
  -t free \
  --profile <PROFILE> \
  --var="sql_warehouse_id=<WAREHOUSE_ID>"
databricks bundle deploy \
  -t free \
  --profile <PROFILE> \
  --var="sql_warehouse_id=<WAREHOUSE_ID>"
```

Para avaliar novamente a qualidade sem refazer a carga:

```bash
cd src/databricks
databricks bundle run quality_monitoring \
  --profile <PROFILE> \
  --var="sql_warehouse_id=<WAREHOUSE_ID>"
```

Uma atualização completa da Pipeline deve ser usada apenas quando houver
mudança de lógica, schema ou necessidade explícita de recomputar as tabelas:

```bash
cd src/databricks
databricks bundle run medallion \
  --full-refresh-all \
  --profile <PROFILE> \
  --var="sql_warehouse_id=<WAREHOUSE_ID>"
```

Para usar ingestão contínua, altere o parâmetro `fim` para `auto` e ative o
schedule na interface ou no recurso do Bundle. A ampliação da Landing e da
Bronze não muda o recorte de consumo. Ajuste também
`ifood.silver.end_month` e, quando necessário,
`ifood.analysis.passenger_reference_month` em
`resources/medallion_pipeline.yml`.
