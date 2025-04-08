# 🧪 MVP – Engenharia de Dados (PUC-Rio)

Este projeto desenvolve um pipeline de dados para analisar a correlação entre o IPCA, a SELIC e o IBOVESPA nos últimos 15 anos. A estrutura foi implementada no Databricks Community Edition, utilizando boas práticas de engenharia de dados.

## 🎯 Objetivo

Construir um pipeline de dados completo em nuvem, seguindo as camadas BRONZE, SILVER e GOLD, realizar modelagem estrela para consultas analíticas, aplicar análises estatísticas e gerar insights econômicos.

## 🛠️ Estrutura do Projeto

- **Camada BRONZE:** Leitura e armazenamento dos dados brutos de IPCA, SELIC e IBOVESPA.
- **Camada SILVER:** Limpeza e padronização dos dados, garantindo qualidade para análises.
- **Camada GOLD:** Organização dos dados tratados em tabelas Delta registradas no metastore.
- **Modelagem Estrela:** Implementação de uma tabela fato e tabelas dimensão para otimizar consultas.
- **Consultas SQL:** Realização de JOINs entre fato e dimensões para análises específicas.
- **Análises Estatísticas:** Criação de matriz de correlação e aplicação de regressão linear simples.
- **Visualizações:** Geração de gráficos para interpretação dos resultados.

## 📂 Estrutura de Pastas


## 📓 Notebook Principal

- [`Final_MVP.ipynb`](./Final_MVP.ipynb): Notebook completo que contém todas as etapas do projeto, desde o carregamento dos dados até a análise estatística final.

## 🖼️ Prints

Prints dos principais resultados estão disponíveis na pasta [`prints/`](./prints/):

- Criação e visualização da Tabela GOLD
- Consultas SQL com JOINs
- Matriz de Correlação
- Gráfico de Regressão Linear

## 🚀 Como Executar

1. Subir os arquivos de dados para o Databricks.
2. Executar o notebook `Final_MVP.ipynb`.
3. Validar a criação das camadas BRONZE, SILVER e GOLD.
4. Analisar os resultados e visualizações geradas.

## 📚 Tecnologias Utilizadas

- **Databricks Community Edition**
- **Apache Spark (PySpark e SQL)**
- **Python (Pandas, Seaborn, Matplotlib)**
- **Delta Lake**
- **GitHub**

## 📈 Resultados

- Matriz de correlação demonstrando a relação entre IPCA, SELIC e IBOVESPA.
- Regressão linear entre SELIC e IBOVESPA indicando tendência negativa.
- Estrutura de dados pronta para reuso e futuras análises.

## ✅ Conclusão

Todas as etapas propostas foram implementadas com sucesso, utilizando boas práticas de engenharia de dados, modelagem analítica e análise estatística. O pipeline está estruturado de forma a permitir a expansão e reuso dos dados no Databricks.

---


