# MVP - Engenharia de Dados (PUC-Rio)

## 🎯 Objetivo
Analisar a correlação entre três indicadores econômicos brasileiros — SELIC, IPCA e IBOVESPA — ao longo dos últimos 15 anos.

### Perguntas de negócio:
- Existe correlação entre a variação da SELIC e o IBOVESPA?
- A inflação (IPCA) influencia o desempenho do IBOVESPA?
- Como a SELIC impacta a inflação?
- Qual indicador tem maior influência sobre o mercado acionário?

---

## 🧱 Pipeline de Dados

| Etapa  | Descrição |
|--------|-----------|
| Bronze | Leitura dos arquivos `.csv` brutos |
| Silver | Tratamento e junção dos dados |
| Gold   | Análise exploratória, correlação, visualização, modelagem e exportação |

---

## 📊 Tecnologias utilizadas
- Databricks Community Edition
- Apache Spark (PySpark)
- Python (pandas, seaborn, matplotlib)
- Spark SQL
- GitHub

---

## 📈 Análises Visuais

### Gráfico de Linha – IPCA e SELIC
Evolução mensal dos indicadores ao longo do tempo. A SELIC variou em ciclos e o IPCA oscilou em sincronia parcial. Correlação levemente negativa.

### Gráfico de Linha – IBOVESPA
Evolução do índice IBOVESPA mostra ciclos de mercado. Influência dos indicadores macroeconômicos é visualmente perceptível em alguns períodos.

### Dispersão – SELIC vs IBOVESPA
Distribuição de pontos sem padrão definido, confirmando correlação quase nula.

### Dispersão – IPCA vs IBOVESPA
Tendência levemente negativa. IPCA elevado tende a coincidir com quedas no IBOVESPA.

---

## 🧮 Modelagem Relacional com SQL

Foram criadas as seguintes tabelas:

- `dim_tempo(data_id, data, ano, mes)`
- `dim_ipca(ipca_id, ipca_valor)`
- `dim_selic(selic_id, selic_valor)`
- `dim_ibov(ibov_id, ibov_valor)`
- `fato_economia(data_id, ipca_id, selic_id, ibov_id)`

As relações entre elas foram feitas via `JOIN`, simulando um modelo estrela. Apesar de o Spark SQL não suportar `FOREIGN KEY` e `PRIMARY KEY`, os relacionamentos foram estruturados logicamente.

---

## 🧾 Estrutura do Repositório

