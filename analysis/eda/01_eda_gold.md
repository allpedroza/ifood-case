# EDA — NYC TLC Yellow Taxi

Este documento acompanha o notebook
[`01_eda_gold.sql`](01_eda_gold.sql) e registra os resultados obtidos em 27 de julho de 2026.

Fonte principal:
`case_ifood.tlc_data_gold.gold_yellow_trips_consumption`.

O período analisado compreende os arquivos de referência de janeiro a maio de
2023. O mês é determinado por `_reference_month`, e não pelo timestamp da
viagem, para que registros temporalmente anômalos continuem associados ao
arquivo que os forneceu.

Os valores abaixo são um snapshot. O notebook SQL é a fonte executável e deve
ser executado novamente após mudanças nos dados ou na pipeline.

## 1. Evolução mensal da quantidade de corridas

| Mês de referência | Corridas | Variação mensal |
|---|---:|---:|
| 2023-01 | 3.066.766 | — |
| 2023-02 | 2.913.955 | -4,98% |
| 2023-03 | 3.403.766 | 16,81% |
| 2023-04 | 3.288.250 | -3,39% |
| 2023-05 | 3.513.649 | 6,85% |

Foram analisadas **16.186.386 corridas**. Fevereiro e abril apresentaram
redução em relação ao mês anterior; maio teve o maior volume, com 3.513.649
corridas.

### Visão adicional pelo mês de pickup

Esta visão usa `tpep_pickup_datetime` como referência de negócio e considera
somente pickups entre `2023-01-01` e `2023-06-01`. Ela não substitui
`_reference_month`: as duas perspectivas respondem a perguntas diferentes.

- `_reference_month`: mês declarado pelo arquivo e usado para rastrear a carga;
- mês de pickup: mês em que a corrida efetivamente ocorreu.

| Mês | Corridas pelo arquivo | Corridas pelo pickup | Diferença | Variação mensal pelo pickup |
|---|---:|---:|---:|---:|
| 2023-01 | 3.066.766 | 3.066.726 | -40 | — |
| 2023-02 | 2.913.955 | 2.914.003 | 48 | -4,98% |
| 2023-03 | 3.403.766 | 3.403.660 | -106 | 16,80% |
| 2023-04 | 3.288.250 | 3.288.248 | -2 | -3,39% |
| 2023-05 | 3.513.649 | 3.513.645 | -4 | 6,85% |

As diferenças são pequenas, mas confirmam que o mês do arquivo e o mês do
evento não são equivalentes. Há **104 registros a mais na visão por arquivo**
do que na visão de pickups dentro da janela. Além disso, registros podem migrar
entre meses adjacentes quando agrupados pelo pickup, motivo pelo qual as
diferenças mensais não devem ser interpretadas apenas como exclusões.

Para indicadores operacionais de ingestão e reconciliação, deve-se usar
`_reference_month`. Para evolução da demanda e comportamento das corridas,
faz mais sentido usar o mês de `tpep_pickup_datetime`, mantendo as regras de DQ
para monitorar eventos fora do mês declarado pelo arquivo.

## 2. Receita total e ticket médio por mês

`total_amount` é tratado como o valor total registrado pela NYC TLC, e não como
receita financeira liquidada. Ajustes negativos da fonte são preservados.

| Mês | Receita total registrada | Ticket médio | Corridas com valor | Sem valor |
|---|---:|---:|---:|---:|
| 2023-01 | 82.865.192,22 | 27,02 | 3.066.766 | 0 |
| 2023-02 | 78.380.973,40 | 26,90 | 2.913.955 | 0 |
| 2023-03 | 94.636.357,06 | 27,80 | 3.403.766 | 0 |
| 2023-04 | 92.957.238,38 | 28,27 | 3.288.250 | 0 |
| 2023-05 | 101.765.751,96 | 28,96 | 3.513.649 | 0 |

Maio concentrou o maior volume, a maior receita registrada e o maior ticket
médio. Não houve `total_amount` nulo na janela analisada.

### 2.1 Receita e ticket médio sem valores negativos

Esta visão é uma análise de sensibilidade. Ela mantém `total_amount = 0` e
remove somente registros com `total_amount < 0`. O cenário não substitui a
visão original nem afirma que os valores negativos são erros, pois eles podem
representar ajustes ou estornos.

| Mês | Receita sem negativos | Ticket médio | Corridas consideradas | Negativas removidas | Impacto na receita |
|---|---:|---:|---:|---:|---:|
| 2023-01 | 83.470.189,06 | 27,44 | 3.041.562 | 25.204 | 604.996,84 |
| 2023-02 | 78.974.247,69 | 27,34 | 2.889.055 | 24.900 | 593.274,29 |
| 2023-03 | 95.365.471,15 | 28,26 | 3.374.003 | 29.763 | 729.114,09 |
| 2023-04 | 93.710.028,87 | 28,76 | 3.258.487 | 29.763 | 752.790,49 |
| 2023-05 | 102.573.881,02 | 29,46 | 3.481.872 | 31.777 | 808.129,06 |

A remoção de negativos aumenta tanto a receita somada quanto o ticket médio.
Na janela completa, foram removidas **141.407 corridas**, e a diferença
acumulada da receita foi de **3.488.304,77**. Esse aumento não representa nova
receita: é apenas o efeito matemático de retirar valores negativos da soma.

## 3. Corridas e receita por VendorID

| VendorID | Corridas | Participação | Receita registrada | Ticket médio |
|---:|---:|---:|---:|---:|
| 2 | 11.809.794 | 72,96% | 333.865.111,70 | 28,27 |
| 1 | 4.372.609 | 27,01% | 116.560.246,08 | 26,66 |
| 6 | 3.983 | 0,02% | 180.155,24 | 45,23 |

O VendorID 2 respondeu por aproximadamente 73% das corridas e apresentou
ticket médio superior ao VendorID 1. O VendorID 6 teve participação residual;
seu ticket médio mais alto deve ser interpretado considerando o volume muito
menor. Não foram encontrados `VendorID` nulos.

### 3.1 Corridas e receita por VendorID sem valores negativos

| VendorID | Corridas consideradas | Negativas removidas | Receita sem negativos | Ticket médio | Impacto na receita |
|---:|---:|---:|---:|---:|---:|
| 2 | 11.668.387 | 141.407 | 337.353.416,47 | 28,91 | 3.488.304,77 |
| 1 | 4.372.609 | 0 | 116.560.246,08 | 26,66 | 0,00 |
| 6 | 3.983 | 0 | 180.155,24 | 45,23 | 0,00 |

Todos os valores negativos da janela estão associados ao VendorID 2. Portanto,
a alteração entre os cenários original e sem negativos está concentrada nesse
fornecedor. Isso justifica manter as duas visões: a original representa
fielmente a fonte, enquanto a alternativa evidencia a sensibilidade das
métricas aos ajustes negativos.

## 4. Média de passageiros por corrida

`media_passageiros_informada` considera todos os valores não nulos.
`media_passageiros_validos` considera somente `passenger_count > 0`.

| Mês | Média informada | Média válida | Corridas | Passageiros informados | Nulos | Não positivos |
|---|---:|---:|---:|---:|---:|---:|
| 2023-01 | 1,36 | 1,39 | 3.066.766 | 2.995.023 | 71.743 | 51.164 |
| 2023-02 | 1,35 | 1,38 | 2.913.955 | 2.837.138 | 76.817 | 47.277 |
| 2023-03 | 1,35 | 1,38 | 3.403.766 | 3.316.147 | 87.619 | 58.365 |
| 2023-04 | 1,38 | 1,41 | 3.288.250 | 3.197.560 | 90.690 | 56.950 |
| 2023-05 | 1,36 | 1,38 | 3.513.649 | 3.411.853 | 101.796 | 59.725 |

A média válida permaneceu entre 1,38 e 1,41 passageiro. A diferença entre as
duas médias demonstra o impacto dos valores não positivos. Os nulos não foram
imputados e permanecem monitorados pelas regras de completude.

## 5. Distribuição da duração das corridas

Duração é a diferença, em minutos, entre o desembarque e o embarque.

| Faixa | Corridas | Participação |
|---|---:|---:|
| Negativa | 795 | 0,0049% |
| Até 5 min | 1.921.264 | 11,8696% |
| 5 a 15 min | 8.042.939 | 49,6895% |
| 15 a 30 min | 4.521.792 | 27,9358% |
| 30 a 60 min | 1.473.542 | 9,1036% |
| 1 a 2 h | 207.258 | 1,2804% |
| 2 a 24 h | 18.702 | 0,1155% |
| Acima de 24 h | 94 | 0,0006% |

Aproximadamente metade das viagens durou entre 5 e 15 minutos. Foram
encontradas 795 cronologias negativas e 94 durações superiores a 24 horas;
ambas são preservadas para auditoria e sinalizadas por Data Quality.

## 6. Registros rejeitados ou sinalizados por Data Quality

O pipeline não rejeita fisicamente registros. As regras monitoram violações e
preservam os dados nas quatro camadas. A execução mais recente avaliou
16.186.386 registros Yellow por regra e apresentou os mesmos resultados em
Landing, Bronze, Silver e Gold:

| Regra | Registros sinalizados | Percentual | Status |
|---|---:|---:|---|
| `passenger_count_null` | 428.665 | 2,648306% | WARN |
| `passenger_count_non_positive` | 273.481 | 1,689574% | WARN |
| `total_amount_non_positive` | 144.146 | 0,890539% | WARN |
| `trip_datetime_after_reference_month` | 4.992 | 0,030841% | WARN |
| `invalid_trip_chronology` | 795 | 0,004912% | WARN |
| `pickup_before_reference_month` | 217 | 0,001341% | WARN |
| `trip_duration_over_24_hours` | 94 | 0,000581% | WARN |
| `vendor_id_null` | 0 | 0,000000% | OK |
| `total_amount_null` | 0 | 0,000000% | OK |

Somando as avaliações, cada camada teve 852.390 sinalizações em 145.677.474
pares registro-regra, equivalentes a aproximadamente **0,5851% das
verificações**. Esse valor não representa registros únicos: uma mesma corrida
pode violar mais de uma regra.

A igualdade entre as quatro camadas indica que as transformações preservaram
essas ocorrências; não há evidência de que elas tenham sido introduzidas entre
Landing e Gold.

## 7. Relação entre duração e valor total

Para a correlação, foram consideradas somente viagens com cronologia válida,
duração entre zero e 24 horas e `total_amount` informado.

- corridas consideradas: **16.185.497**;
- correlação de Pearson entre duração e valor: **0,2330**.

A associação é positiva, mas fraca: viagens mais longas tendem a apresentar
valores maiores, porém duração isoladamente explica pouco da variação de
`total_amount`.

| Faixa de duração | Corridas | Duração média | Valor médio | Mediana do valor |
|---|---:|---:|---:|---:|
| Até 5 min | 1.921.264 | 3,32 min | 13,72 | 12,12 |
| 5 a 15 min | 8.042.939 | 9,68 min | 18,32 | 17,88 |
| 15 a 30 min | 4.521.792 | 20,67 min | 33,85 | 29,40 |
| 30 a 60 min | 1.473.542 | 40,14 min | 70,22 | 72,30 |
| 1 a 2 h | 207.258 | 71,76 min | 93,64 | 92,30 |
| 2 a 24 h | 18.702 | 1.087,91 min | 47,49 | 25,75 |

O crescimento do valor é consistente até a faixa de duas horas. A faixa entre
2 e 24 horas apresenta duração média próxima de 18 horas e redução relevante
de valor, comportamento compatível com timestamps extremos ou outras
anomalias. Ela deve ser investigada separadamente e não interpretada como uma
relação operacional linear.

## Como reproduzir e validar?

Para atualizar o snapshot:

1. execute a pipeline e o Job de Data Quality;
2. execute todas as células de `01_eda_gold.sql` no Databricks SQL;
3. substitua os resultados deste documento somente pelos valores retornados;
4. registre a nova data de execução e revise as interpretações.
