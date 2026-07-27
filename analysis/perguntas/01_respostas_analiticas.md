# Respostas analíticas: NYC TLC

Este documento acompanha o notebook
[`01_respostas_analiticas.sql`](01_respostas_analiticas.sql) e registra os
resultados obtidos no Databricks em 27 de julho de 2026.

Fontes:

- `case_ifood.tlc_data_gold.gold_yellow_trips_consumption`;
- `case_ifood.tlc_data_gold.gold_taxi_passengers_by_hour`.

Os valores representam um snapshot das tabelas Gold no momento da execução.
Uma recomputação da pipeline pode alterar os resultados caso os arquivos de
origem ou as regras do produto de dados sejam atualizados. O SQL versionado é a
fonte executável; este Markdown é o registro interpretativo das respostas.

## 1. Média de valor total por mês para Yellow Taxi

### Premissa

Cada registro representa uma viagem. A métrica é a média de `total_amount` por
viagem em cada mês, e não a soma da receita da frota. O `AVG` desconsidera
valores nulos e preserva valores negativos existentes na fonte, pois eles podem
representar ajustes operacionais.

### Resultado

| Mês | Média de `total_amount` | Viagens com valor informado |
|---|---:|---:|
| 2023-01 | 27,02 | 3.066.726 |
| 2023-02 | 26,90 | 2.914.003 |
| 2023-03 | 27,80 | 3.403.660 |
| 2023-04 | 28,27 | 3.288.248 |
| 2023-05 | 28,96 | 3.513.645 |

### Resposta

Entre janeiro e maio de 2023, a média mensal por viagem variou de 26,90,
em fevereiro, a 28,96, em maio. Depois da redução observada em fevereiro,
a média aumentou em março, abril e maio.

## 2. Média de passageiros por hora no mês de maio

### Premissa

"Todos os táxis da frota" é interpretado no sentido regulatório da NYC TLC:
Yellow Taxi e Green Taxi. FHV e High Volume FHV não são classificados como
táxi nesse contexto e não disponibilizam `passenger_count`.

A média considera somente viagens com `passenger_count > 0`. Valores nulos,
iguais a zero ou negativos são excluídos do denominador e apresentados como
viagens descartadas. Yellow utiliza `tpep_pickup_datetime` e Green utiliza
`lpep_pickup_datetime`; ambos são harmonizados como horário de embarque.

### Resultado

| Hora | Média de passageiros | Viagens consideradas | Yellow | Green | Descartadas |
|---:|---:|---:|---:|---:|---:|
| 00 | 1,43 | 90.848 | 89.782 | 1.066 | 4.414 |
| 01 | 1,43 | 59.143 | 58.390 | 753 | 3.005 |
| 02 | 1,45 | 38.179 | 37.614 | 565 | 2.119 |
| 03 | 1,45 | 25.048 | 24.571 | 477 | 1.548 |
| 04 | 1,40 | 16.479 | 16.131 | 348 | 1.953 |
| 05 | 1,28 | 18.861 | 18.481 | 380 | 1.893 |
| 06 | 1,26 | 46.937 | 45.887 | 1.050 | 3.755 |
| 07 | 1,28 | 94.761 | 92.325 | 2.436 | 7.501 |
| 08 | 1,29 | 129.182 | 126.208 | 2.974 | 9.658 |
| 09 | 1,31 | 145.086 | 141.807 | 3.279 | 8.702 |
| 10 | 1,35 | 157.936 | 154.749 | 3.187 | 8.081 |
| 11 | 1,36 | 172.091 | 168.620 | 3.471 | 8.280 |
| 12 | 1,37 | 185.481 | 181.923 | 3.558 | 8.675 |
| 13 | 1,38 | 189.536 | 186.197 | 3.339 | 8.973 |
| 14 | 1,39 | 206.538 | 202.516 | 4.022 | 9.797 |
| 15 | 1,40 | 211.294 | 206.927 | 4.367 | 10.009 |
| 16 | 1,40 | 211.784 | 207.049 | 4.735 | 9.705 |
| 17 | 1,39 | 231.006 | 226.111 | 4.895 | 10.454 |
| 18 | 1,38 | 245.268 | 240.225 | 5.043 | 10.601 |
| 19 | 1,39 | 219.880 | 215.799 | 4.081 | 8.685 |
| 20 | 1,40 | 195.045 | 191.841 | 3.204 | 7.136 |
| 21 | 1,42 | 198.770 | 195.990 | 2.780 | 7.463 |
| 22 | 1,43 | 183.506 | 181.332 | 2.174 | 7.842 |
| 23 | 1,42 | 143.273 | 141.649 | 1.624 | 6.632 |

### Resposta

A média permaneceu próxima de um a dois passageiros durante todo o dia,
variando de 1,26 passageiro às 06h a 1,45 passageiro às 02h e às 03h.
O maior volume válido ocorreu às 18h, com 245.268 viagens consideradas.
Em todas as horas, Yellow representou a maior parte das viagens, enquanto
Green permaneceu incluído para preservar o conceito regulatório de todos os
táxis.

## Como reproduzir e validar? 

Para atualizar este documento, execute todas as células de
`01_respostas_analiticas.sql` no Databricks SQL após a atualização da pipeline
e substitua o snapshot somente por resultados efetivamente retornados pelas
tabelas Gold.
