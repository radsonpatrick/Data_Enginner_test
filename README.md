# ANP Fuel Sales ETL Test

## Objective

This test consists in developing an ETL pipeline to extract internal pivot caches from consolidated reports made available by Brazilian government's regulatory agency for oil/fuels, ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis).
### Goal

This `xls` file has some pivot tables like this one:

![Pivot Table](./images/pivot.png)

The developed pipeline is meant to extract and structure the underlying data of two of these tables:
- Sales of oil derivative fuels by UF and product
- Sales of diesel by UF and type

The totals of the extracted data must be equal to the totals of the pivot tables.

### Schema

Data should be stored in the following format:

| Column       | Type        |
| ------------ | ----------- |
| `year_month` | `date`      |
| `uf`         | `string`    |
| `product`    | `string`    |
| `unit`       | `string`    |
| `volume`     | `double`    |
| `created_at` | `timestamp` |

# Solution
Para a solução fiz todo o desenvolvimento em docker  e o orquestrador ultilizado foi  o Airflow:

![Aiflow](./images/airflow.jpg)
Gravando os dados no banco de dados Mysql:

![mysql](./images/mysql.jpg)

Com a seguinte estrutura para ambas tabelas :

![estrutura](./images/diesel.jpg)

A quantidade de linhas é a seguinte:

`dev_fuel` = 54.432

`diesel` = 12.960

Tambem fiz o armazenamento do dados em  [Data_Enginner_test/data](https://github.com/radsonpatrick/Data_Enginner_test/tree/main/data) lá temos output em `csv` das tabelas de combustiveis derivados [dev_fuel.csv](https://github.com/radsonpatrick/Data_Enginner_test/blob/main/data/dev_fuel.csv) e a de diesel [diesel.csv](https://github.com/radsonpatrick/Data_Enginner_test/blob/main/data/diesel.csv) 

# How to run
`git clone https://github.com/radsonpatrick/Data_Enginner_test.git`

`cd Data_Enginner_test`

`docker-compose up airflow-init`

`docker-compose up`

After installation acess on browser `http://localhost:8080`
