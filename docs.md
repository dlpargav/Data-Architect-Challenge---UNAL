# 📊 SNIES Data Pipeline — Academic Capacity Monitoring (Bogotá)

## 🧠 Overview

This project implements an end-to-end data pipeline to monitor the **academic capacity of Higher Education Institutions (IES) in Bogotá**, using open data from SNIES.

The main objective is to compute and analyze the:

> **Relación Estudiantes por Docente** (Student-to-Teacher Ratio)

for the period **2022–2024**.

---

## 🎯 Business Requirements

- **Period:** 2022–2024
- **Geography:** IES located in Bogotá
- **Main Metric:**
  [
  \text{Relación} = \frac{\text{Estudiantes Matriculados}}{\text{Número de Docentes}}
  ]
- **Optional (pending):** Classification of institutions as part of SUE (Sistema Universitario Estatal)

---

## 🏗️ Architecture Overview

The pipeline follows a modular ETL structure:

```text
Raw Data → Cleaning → Transformation → Integration → KPI Calculation
```

### Layers

- **Ingestion:** Load Excel files from SNIES
- **Cleaning:** Normalize column names and formats
- **Transformation:** Filter, aggregate, and structure data
- **Integration:** Join datasets (students + teachers)
- **Output (Gold Layer):** Final dataset ready for BI tools

---

## 📁 Project Structure

```text
project/
│
├── src/
│   ├── processing/
│   │   ├── cleaning.py
│   │   ├── aggregation.py
│   │   ├── docentes.py
│   │   └── estudiantes.py
│   │
│   ├── pipeline/
│   │   └── main.py
│   │
│   └── utils/
│       └── config.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── gold/
```

---

## 🔧 Data Sources

Data is sourced from **SNIES consolidated datasets**, which provide:

- Institution-level data
- Student enrollment (matriculados)
- Teaching staff (docentes)

---

## ⚙️ Data Processing Steps

### 1. Ingestion

Data is loaded from Excel files:

```python
pd.read_excel(file_path, skiprows=7)
```

- `skiprows` handles metadata rows in SNIES files

---

### 2. Column Normalization

Column names are standardized to ensure consistency:

- Remove line breaks
- Collapse spaces
- Convert to lowercase
- Remove accents
- Convert to snake_case

Example:

```text
"CÓDIGO DE LA INSTITUCIÓN" → "codigo_de_la_institucion"
```

---

### 3. Filtering

Data is restricted to Bogotá:

```python
df["municipio_de_domicilio_de_la_ies"].str.contains("bogot", case=False)
```

---

### 4. Aggregation

Both datasets are **highly disaggregated**, so aggregation is required.

#### Docentes:

```python
groupby(["codigo_de_la_institucion", "institucion", "ano"])
→ sum(numero_de_docentes)
```

#### Estudiantes:

```python
groupby(["codigo_de_la_institucion", "institucion", "ano"])
→ sum(numero_de_estudiantes_matriculados)
```

---

### 5. Integration (JOIN)

Datasets are merged using:

```python
on = ["codigo_de_la_institucion", "ano"]
```

- `inner join` ensures only valid pairs are included

---

### 6. KPI Calculation

```python
relacion_estudiantes_por_docente =
    numero_de_estudiantes_matriculados / numero_de_docentes
```

---

## 🔁 Multi-Year Processing

A configuration-driven approach enables scalability:

```python
SNIES_FILES = {
    2022: {...},
    2023: {...},
    2024: {...}
}
```

The pipeline loops through each year:

```python
for year in SNIES_FILES:
    process(year)
```

---

## 🧪 Data Validation

Key validation steps include:

- Checking for missing values
- Identifying mismatches using anti-joins
- Verifying aggregation correctness
- Ensuring realistic KPI values

---

## ⚠️ Key Challenges & Solutions

| Challenge                  | Solution                   |
| -------------------------- | -------------------------- |
| Inconsistent Excel formats | `skiprows` parameterized   |
| Messy column names         | normalization function     |
| Highly disaggregated data  | groupby aggregation        |
| Dataset mismatches         | validation with anti-joins |
| Schema inconsistency       | standardized naming        |

---

## 🧠 Design Decisions

- **Config-driven ingestion** → scalable to new years
- **Reusable functions** → avoid duplication
- **Early filtering** → reduce data size and errors
- **Aggregation before join** → ensures correctness
- **Use of IDs for joins** → avoids name inconsistencies

---

## 📈 Output Dataset (Gold Layer)

Final structure:

```text
codigo_de_la_institucion
institucion
ano
numero_de_estudiantes_matriculados
numero_de_docentes
relacion_estudiantes_por_docente
```

---

## 🚀 Next Steps

- [ ] Persist data in PostgreSQL
- [ ] Connect to Tableau for visualization
- [ ] Add SUE classification
- [ ] Orchestrate pipeline with Airflow
- [ ] Containerize with Docker

---

## 💬 Notes

This pipeline is designed to balance:

- **Scalability**
- **Clarity**
- **Maintainability**

while avoiding unnecessary overengineering.

---
