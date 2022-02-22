# ANP Fuel Sales ETL Test

## Objective

This test consists in developing an ETL pipeline to extract internal pivot caches from consolidated reports made available by Brazilian government's regulatory agency for oil/fuels, ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis).
### Goal

This `xls` file has some pivot tables like this one:

(./images/pivot.png)

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
