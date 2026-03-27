# Pipeline ETL para Faturamento de Ecommerce

O objetivo desse projeto é praticar/demonstrar em um ETL real e funcional com Python, PostgreSQL, Airflow e Docker, extraindo dados do banco de dados e de uma API, os processando, criando um Star Schema e carregando em um DataWarehouse.

---

## Modelagem de dados
O Data Warehouse segue o modelo Star Schema, com:

- Tabela Fato
  - Vendas 
- Tabelas Dimensão
  - Data
  - Produto
  - Cliente
  - Câmbio

---

## Arquitetura

```
pipeline/
│
├── data/                # Dados brutos e processados
├── src/                 # Código fonte
├── dags/                # DAGs do Airflow
├── tests/               # Testes automatizados
├── pyproject.toml       # Dependências 
└── README.md
```
```
src/
│
├── database/            # Conecção com DB e WH
├── extract/             # Scripts de extração
├── transform/           # Scripts de transformação
├── load/                # Scripts de carga
└── utils/               # Logs
```

---

## Pré requisitos

- Python 3.14+
- Docker
- Apache Airflow
- Acesso à internet (API externa)

---

## Instalação

Clone o repositório:

``git clone https://github.com/dosanjosfelipe/ecom-data-pipeline.git ``

``cd pipelineETL``  <br> 

Instale as dependências: 

``uv sync``

---

## Configuração
Crie um arquivo ``.env`` dentro de ``/config`` com as seguintes variáveis:
```
database='ecommerce_etl'
warehouse='ecommerce_wh'
user='SEU NOME DE USUÁRIO POSTGRES'
password='SUA SENHA POSTGRES'
```
E crie outro ``.env`` na raiz do projeto com:
```
AIRFLOW_UID=501
```

---

## Execução

Execute com:
```
docker docompose up
```
Acesse o Airflow:

URL: http://localhost:8080  
Usuário padrão: airflow  
Senha padrão: airflow
---

## Licença

Este projeto está sob a licença MIT.