# 🧪 MVP - Engenharia de Dados (PUC-Rio)

Este repositório contém todas as etapas do MVP da disciplina de Engenharia de Dados da PUC-Rio. O projeto foi desenvolvido na plataforma Databricks Community Edition e envolve a construção de um pipeline de dados com camadas **Bronze**, **Silver** e **Gold**, modelagem em estrela, criação de tabelas no metastore, consultas SQL com JOINs e análise dos dados econômicos brasileiros (IPCA, SELIC e IBOVESPA).

---

## 📁 Estrutura das Camadas

- **Bronze**: Armazenamento bruto dos dados CSV (IPCA, SELIC, IBOV).
- **Silver**: Junção e tratamento dos dados.
- **Gold**: Dados analíticos finais utilizados para visualizações e exportações.

---

## 🖼️ Prints - Item 7: Consultas SQL com JOINs

### ✅ 1. Cluster Ativo
📎 ![Cluster Ativo](print_cluster.png)

### ✅ 2. Upload dos Arquivos CSV
📎 ![Upload CSV](print_upload_csv.png)

### ✅ 3. Execução da Camada Bronze
📎 ![Execução Bronze](print_bronze_exec.png)

### ✅ 4. Execução da Camada Silver
📎 ![Execução Silver](print_silver_exec.png)

### ✅ 5. Criação das Tabelas SQL
📎 ![Tabelas SQL](print_sql_tabelas.png)

### ✅ 6. Tabela `fato_economia` Populada
📎 ![Tabela fato_economia](print_fato_economia.png)

### ✅ 7. Resultado da Consulta com JOIN
📎 ![Resultado JOIN](print_sql_join_resultado.png)

### ✅ 8. Matriz de Correlação (Análise GOLD)
📎 ![Matriz de Correlação](print_gold_analise.png)

---

## 📊 Item 8 – Análise com SQL

Consultas SQL foram realizadas diretamente sobre as tabelas do metastore no Databricks. As análises incluem médias, somas, variações e destaques por ano e mês.

### 📎 Prints:
- `print_analise_medias_anuais.png`
- `print_analise_soma_mensal.png`
- `print_analise_maior_selic.png`
- `print_analise_variacao_ibov.png`

---

## 📦 Item 9 – Exportação Final da Camada GOLD

Nesta etapa, os dados tratados da camada GOLD foram exportados em formato `.csv` a partir da tabela `economia_brasileira` criada no metastore.

🔗 [Clique aqui para baixar o arquivo gold_economia.csv](https://community.cloud.databricks.com/files/gold_economia.csv)

📎 ![Exportação GOLD](print_exportacao_gold.png)

---

## ✅ Conclusão

Todas as etapas do projeto foram concluídas com sucesso, utilizando práticas recomendadas de engenharia de dados, consultas SQL, e organização em camadas. O pipeline foi desenvolvido 100% na nuvem, utilizando Databricks e arquivos públicos acessíveis via GitHub.
