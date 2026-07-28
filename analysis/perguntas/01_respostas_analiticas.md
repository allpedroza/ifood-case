# Respostas analíticas: NYC TLC

Os resultados vieram do notebook
[`01_respostas_analiticas.sql`](01_respostas_analiticas.sql), executado no
Databricks em 28 de julho de 2026.

Fontes:

- `case_ifood.tlc_data_gold.gold_yellow_trips_consumption`;
- `case_ifood.tlc_data_gold.gold_taxi_passengers_by_hour`.

Este é um recorte das tabelas Gold naquela execução. Se a origem ou as regras do
produto mudarem, o SQL deve ser executado novamente. O Markdown registra a
leitura dos resultados, enquanto o arquivo SQL continua sendo a fonte
executável.

## 1. Média de valor total por mês para Yellow Taxi

Cada registro representa uma viagem. A métrica é a média de `total_amount` por
viagem em cada mês, e não a soma da receita da frota. O `AVG` desconsidera
valores nulos e preserva valores negativos existentes na fonte, pois eles podem
representar ajustes operacionais.

| Mês | Média de `total_amount` | Viagens com valor informado |
|---|---:|---:|
| 2023-01 | 27,02 | 3.066.726 |
| 2023-02 | 26,90 | 2.914.003 |
| 2023-03 | 27,80 | 3.403.660 |
| 2023-04 | 28,27 | 3.288.248 |
| 2023-05 | 28,96 | 3.513.645 |

Entre janeiro e maio de 2023, a média mensal por viagem variou de 26,90,
em fevereiro, a 28,96, em maio. Depois da redução observada em fevereiro,
a média aumentou em março, abril e maio.

## 2. Média de passageiros por hora no mês de maio

"Todos os táxis da frota" é interpretado no sentido regulatório da NYC TLC:
Yellow Taxi e Green Taxi. FHV e High Volume FHV não são classificados como
táxi nesse contexto e não disponibilizam `passenger_count`.

A média considera somente viagens com `passenger_count > 0`. Valores nulos,
iguais a zero ou negativos são excluídos do denominador e apresentados como
viagens descartadas. Yellow utiliza `tpep_pickup_datetime` e Green utiliza
`lpep_pickup_datetime`; ambos são harmonizados como horário de embarque.

Yellow + Green é a resposta principal. As médias exclusivas de cada serviço
foram acrescentadas como complemento, sem alterar o universo definido para a
pergunta.

| Hora | Yellow + Green | Apenas Yellow | Apenas Green | Viagens consideradas | Yellow | Green | Descartadas |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 00 | 1,43 | 1,43 | 1,38 | 90.848 | 89.782 | 1.066 | 4.414 |
| 01 | 1,43 | 1,44 | 1,33 | 59.143 | 58.390 | 753 | 3.005 |
| 02 | 1,45 | 1,45 | 1,38 | 38.179 | 37.614 | 565 | 2.119 |
| 03 | 1,45 | 1,45 | 1,32 | 25.048 | 24.571 | 477 | 1.548 |
| 04 | 1,40 | 1,40 | 1,35 | 16.479 | 16.131 | 348 | 1.953 |
| 05 | 1,28 | 1,28 | 1,29 | 18.861 | 18.481 | 380 | 1.893 |
| 06 | 1,26 | 1,26 | 1,28 | 46.937 | 45.887 | 1.050 | 3.755 |
| 07 | 1,28 | 1,28 | 1,26 | 94.761 | 92.325 | 2.436 | 7.501 |
| 08 | 1,29 | 1,30 | 1,21 | 129.182 | 126.208 | 2.974 | 9.658 |
| 09 | 1,31 | 1,31 | 1,26 | 145.086 | 141.807 | 3.279 | 8.702 |
| 10 | 1,35 | 1,35 | 1,29 | 157.936 | 154.749 | 3.187 | 8.081 |
| 11 | 1,36 | 1,36 | 1,32 | 172.091 | 168.620 | 3.471 | 8.280 |
| 12 | 1,37 | 1,38 | 1,30 | 185.481 | 181.923 | 3.558 | 8.675 |
| 13 | 1,38 | 1,38 | 1,26 | 189.536 | 186.197 | 3.339 | 8.973 |
| 14 | 1,39 | 1,39 | 1,28 | 206.538 | 202.516 | 4.022 | 9.797 |
| 15 | 1,40 | 1,40 | 1,27 | 211.294 | 206.927 | 4.367 | 10.009 |
| 16 | 1,40 | 1,40 | 1,27 | 211.784 | 207.049 | 4.735 | 9.705 |
| 17 | 1,39 | 1,39 | 1,25 | 231.006 | 226.111 | 4.895 | 10.454 |
| 18 | 1,38 | 1,38 | 1,27 | 245.268 | 240.225 | 5.043 | 10.601 |
| 19 | 1,39 | 1,39 | 1,27 | 219.880 | 215.799 | 4.081 | 8.685 |
| 20 | 1,40 | 1,40 | 1,30 | 195.045 | 191.841 | 3.204 | 7.136 |
| 21 | 1,42 | 1,42 | 1,30 | 198.770 | 195.990 | 2.780 | 7.463 |
| 22 | 1,43 | 1,43 | 1,34 | 183.506 | 181.332 | 2.174 | 7.842 |
| 23 | 1,42 | 1,42 | 1,33 | 143.273 | 141.649 | 1.624 | 6.632 |

A média permaneceu próxima de um a dois passageiros durante todo o dia,
variando de 1,26 passageiro às 06h a 1,45 passageiro às 02h e às 03h.
O maior volume válido ocorreu às 18h, com 245.268 viagens consideradas.
Em todas as horas, Yellow representou a maior parte das viagens, enquanto
Green permaneceu incluído para preservar o conceito regulatório de todos os
táxis. A média Green foi menor que a Yellow em 22 das 24 horas. Às 05h e às
06h ela foi ligeiramente maior, e às 07h ficou próxima. Como Green responde
por uma parcela pequena das viagens válidas, a média conjunta acompanha de
perto a média Yellow.

## Reproduzir os resultados

Para atualizar este documento, execute todas as células de
`01_respostas_analiticas.sql` no Databricks SQL após a atualização da pipeline
e substitua o snapshot somente por resultados efetivamente retornados pelas
tabelas Gold.
