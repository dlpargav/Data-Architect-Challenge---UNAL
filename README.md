# Monitor de Capacidad Académica SNIES

> Pipeline automatizado end-to-end para el monitoreo de la **relación estudiante-docente** en las Instituciones de Educación Superior (IES) de Bogotá, utilizando datos abiertos del sistema SNIES de Colombia.

**Periodo de análisis:** 2022 - 2024 · **Alcance:** IES de Bogota · **Métrica principal:** `Estudiantes Matriculados / Docentes`

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          docker-compose                                     │
│                                                                             │
│  ┌──────────────────────────────────────┐   ┌───────────────────────────┐  │
│  │         Contenedor del Pipeline      │   │     Servidor Prefect       │  │
│  │         (Python 3.11 + uv)           │   │     UI en :4200            │  │
│  │                                      │   │   (--profile monitoring)   │  │
│  │  src/                                │   └───────────────────────────┘  │
│  │  ├── ingestion/   <- descarga + cache│                                  │
│  │  ├── processing/  <- limpieza, DQ    │                                  │
│  │  ├── loading/     <- escritura BD    │                                  │
│  │  └── orchestration/ <- flujo Prefect │                                  │
│  └───────────────┬──────────────────────┘                                  │
│                  │ SQLAlchemy + psycopg2                                    │
│                  ▼                                                          │
│  ┌───────────────────────────────────────┐                                  │
│  │           PostgreSQL 16               │  <- puerto 5432 (Tableau)       │
│  │                                       │                                  │
│  │  bronze.docentes_raw                  │                                  │
│  │  bronze.estudiantes_raw               │                                  │
│  │                                       │                                  │
│  │  gold.dim_institucion  (+ flag es_sue)│                                  │
│  │  gold.dim_periodo                     │                                  │
│  │  gold.fact_capacidad_academica        │                                  │
│  └───────────────────────────────────────┘                                  │
│                                                                             │
│  Externo: Tableau Desktop -> localhost:5432 -> esquema gold                │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Flujo de Datos (Arquitectura Medallion)

```
Portal SNIES (URL)
       │
       ▼  [cache: data/raw/]
   BRONZE  <--- Archivos crudos Excel/CSV, copia 1:1 + ingestion_timestamp
       │
       ▼  [normalizacion · filtro Bogota · validaciones de calidad (DQ)]
   SILVER  <--- DataFrames limpios, tipados y validados (en memoria)
       │
       ▼  [cruces · calculo KPI · clasificacion SUE · upsert]
    GOLD   <--- Esquema estrella: dim_institucion · dim_periodo · fact_capacidad_academica
       │
       ▼
  Tableau Desktop (conexion directa al esquema gold en PostgreSQL)
```

---

## Instalación y Ejecución

### Prerrequisitos

| Herramienta     | Versión    | Propósito                   |
| --------------- | ---------- | --------------------------- |
| Docker Desktop  | >= 4.x     | Ejecuta todos los servicios |
| Tableau Desktop | Cualquiera | Visualización de BI         |

---

### Despliegue con Docker (Totalmente reproducible)

```bash
# 1. Clonar el repositorio
git clone https://github.com/dlpargav/Data-Architect-Challenge---UNAL.git
cd snies-pipeline

# 2. Iniciar PostgreSQL, ejecutar Docker Desktop  y ejecutar el pipeline
docker compose up --build

# 3. (Opcional) Ejecutar con la interfaz de monitoreo de Prefect en http://localhost:4200
docker compose --profile monitoring up --build

# 4. Volver a ejecutar para un año especifico únicamente (carga incremental)
docker compose run pipeline python -m src.pipeline.main --years 2024
```

**Que sucede en la primera ejecución:**

1. PostgreSQL se inicia y crea los esquemas `bronze`, `silver` y `gold`.
2. El pipeline descarga los archivos del SNIES (omite este paso si ya estan cacheados en `data/raw/`).
3. Los datos crudos se cargan en las tablas `bronze.*`.
4. Los datos se limpian, se filtran para Bogotá y pasan por controles de calidad de datos (DQ).
5. Se construye el esquema estrella gold con el KPI `relacion_estudiantes_por_docente` y el indicador `es_sue`.

---

### Agregar un Nuevo Periodo

Para incluir un nuevo año (ej. 2025):

1. Abrir `src/utils/config.py`.
2. Agregar una nueva entrada a `SNIES_FILES`:

```python
2025: {
    "docentes": {
        "url": "https://snies.mineducacion.gov.co/1778/articles-XXXXX_recurso.xlsx",
        "local_name": "Docentes 2025.xlsx",
        "skiprows": 5,
        "sheet_name": "1.",
    },
    "estudiantes": {
        "url": "https://snies.mineducacion.gov.co/1778/articles-XXXXX_recurso_2.xlsx",
        "local_name": "Estudiantes 2025.xlsx",
        "skiprows": 5,
        "sheet_name": "1.",
    },
},
```

3. Ejecutar: `docker compose run pipeline python -m src.pipeline.main --years 2025`

No se requieren otros cambios en el codigo.

---

## Conexión con Tableau

| Parámetro     | Valor       |
| ------------- | ----------- |
| Servidor      | `localhost` |
| Puerto        | `5432`      |
| Base de Datos | `snies_db`  |
| Usuario       | `snies`     |
| Contraseña    | `snies`     |
| Esquema       | `gold`      |

**Vista inicial recomendada** — pegue esto en el Custom SQL de Tableau:

```sql
SELECT
    f.id,
    i.nombre_institucion,
    i.es_sue,
    p.ano,
    p.semestre,
    f.total_estudiantes_matriculados,
    f.total_docentes,
    f.relacion_estudiantes_por_docente
FROM gold.fact_capacidad_academica f
JOIN gold.dim_institucion i ON i.codigo_institucion = f.codigo_institucion
JOIN gold.dim_periodo     p ON p.id_periodo         = f.id_periodo
ORDER BY p.ano, i.nombre_institucion;
```

---

## Consultas de Verificación

Despues de ejecutar el pipeline, valide la salida con:

```sql
-- 1. Conteo de filas por anio (debe tener datos para 2022, 2023, 2024)
SELECT p.ano, COUNT(*) AS instituciones
FROM gold.fact_capacidad_academica f
JOIN gold.dim_periodo p ON p.id_periodo = f.id_periodo
GROUP BY p.ano ORDER BY p.ano;

-- 2. Instituciones SUE (debe incluir ~5 universidades en Bogota)
SELECT nombre_institucion, es_sue
FROM gold.dim_institucion
WHERE es_sue = TRUE ORDER BY nombre_institucion;

-- 3. Verificacion del KPI (marca valores atípicos fuera del rango [1, 200])
SELECT i.nombre_institucion, p.ano, f.relacion_estudiantes_por_docente
FROM gold.fact_capacidad_academica f
JOIN gold.dim_institucion i ON i.codigo_institucion = f.codigo_institucion
JOIN gold.dim_periodo p ON p.id_periodo = f.id_periodo
WHERE f.relacion_estudiantes_por_docente < 1
   OR f.relacion_estudiantes_por_docente > 200;

-- 4. Top 10 instituciones por relación estudiante-docente (ultimo año)
SELECT i.nombre_institucion, i.es_sue, f.relacion_estudiantes_por_docente
FROM gold.fact_capacidad_academica f
JOIN gold.dim_institucion i ON i.codigo_institucion = f.codigo_institucion
JOIN gold.dim_periodo p ON p.id_periodo = f.id_periodo
WHERE p.ano = (SELECT MAX(ano) FROM gold.dim_periodo)
ORDER BY f.relacion_estudiantes_por_docente DESC
LIMIT 10;
```

---

## Decisiones Técnicas

### ¿Por qué Arquitectura Medallion (Bronze / Silver / Gold)?

Un pipeline plano que lee -> transforma -> escribe en una sola pasada no tiene trazabilidad y no se puede re-ejecutar parcialmente. El patron medallion ofrece:

- **Bronze**: los datos crudos exactamente como los publicó el SNIES, con marcas de tiempo. Si se encuentra un error en la lógica de limpieza, los datos crudos nunca necesitan volver a descargarse.
- **Silver**: aqui ocurre la limpieza y la verificación de calidad (DQ). Esta es la capa que el equipo de datos utiliza para depurar.
- **Gold**: la única capa que tocan las herramientas de BI. Los KPIs precalculados evitan que Tableau ejecute divisiones costosas en tiempo de consulta.

### ¿Por qué Prefect en lugar de Apache Airflow?

| Factor                      | Prefect                               | Airflow                                                |
| --------------------------- | ------------------------------------- | ------------------------------------------------------ |
| Servicios Docker necesarios | 1 (servidor opcional)                 | 4 (webserver + scheduler + worker + base de metadatos) |
| Modelo de código            | Python puro `@task` / `@flow`         | DSL de DAGs + operadores                               |
| Reintentos                  | `@task(retries=3)`                    | Configuración a nivel de operador                      |
| Cache de tareas             | Integrado (cache de 24h en descargas) | Requiere XCom + lógica personalizada                   |
| Curva de aprendizaje        | Baja                                  | Media-Alta                                             |
| Adecuación a producción     | Equipos < ~10 DE                      | Corporativo / múltiples equipos                        |

Para el alcance de este proyecto, Prefect es la herramienta adecuada. La **ruta de migración a Airflow** es directa: cada `@task` se mapea 1:1 a un `PythonOperator`, y el `@flow` se mapea a un DAG con su `schedule_interval`.

### ¿Por qué `uv` en lugar de `pip` o Poetry?

`uv` es un gestor de paquetes basado en Rust que resuelve e instala dependencias ~10-100 veces mas rápido que pip. El archivo `uv.lock` fija las versiones exactas para la reproducibilidad, y el `Dockerfile` utiliza la imagen base oficial `ghcr.io/astral-sh/uv` — no se necesita pip dentro del contenedor en absoluto.

### ¿Por qué una ingestion controlada por configuración?

El SNIES cambia el formato de sus archivos y las URLs entre publicaciones. Centralizar todos los parámetros específicos del año en `config.py` significa que agregar un nuevo año no requiere cambios en el código, sólo una entrada en la configuración. El parámetro `skiprows` por archivo maneja la variabilidad de los encabezados a lo largo de los años.

### ¿Por qué joins basados en IDs?

Los nombres de las instituciones del SNIES son inconsistentes entre los conjuntos de datos (`"UNIVERSIDAD NACIONAL DE COLOMBIA"` frente a `"Universidad Nacional"` frente a `"Univ. Nacional"`). Cruzar datos a través de `codigo_de_la_institucion` (el ID numerico del SNIES) es determinista e inmune a las diferencias de formato de los nombres. La estandarización de nombres se aplica únicamente con fines de visualización.

### Estrategia de Calidad de Datos (DQ)

| Control                             | Capa   | Acción                                                 |
| ----------------------------------- | ------ | ------------------------------------------------------ |
| Columnas clave nulas                | Silver | Registrar advertencia, excluir fila                    |
| Valores metricos negativos          | Silver | Registrar advertencia                                  |
| Duplicados (institucion, año)      | Silver | Registrar advertencia (groupby lo maneja naturalmente) |
| Instituciones faltantes (anti-join) | Gold   | Registrar conteo de instituciones excluidas            |
| KPI fuera de rango [1, 200]         | Gold   | Registrar advertencia con nombres de instituciones     |
| Columnas esperadas faltantes        | Silver | Levantar `MissingColumnError` (falla rapida)           |

Establezca la variable de entorno `STRICT_DQ=true` para convertir las advertencias en fallos del pipeline — recomendado para procesos de CI.

---

## Estructura del Proyecto

```
snies-pipeline/
│
├── src/
│   ├── ingestion/
│   │   └── loader.py           <- descarga con cache, detección de xlsx/csv
│   ├── processing/
│   │   ├── cleaning.py         <- normalización de columnas + validación de esquemas
│   │   ├── aggregation.py      <- filtro Bogotá + agrupación (flexible por semestre)
│   │   ├── docentes.py         <- transformación Silver para datos de profesores
│   │   ├── estudiantes.py      <- transformación Silver para datos de matriculas
│   │   ├── data_quality.py     <- validaciones DQ (nulos, duplicados, rangos, anti-join)
│   │   └── sue_classifier.py   <- indicador SUE mediante coincidencia normalizada
│   ├── loading/
│   │   ├── models.py           <- ORM SQLAlchemy (esquema estrella Bronze + Gold)
│   │   ├── database.py         <- Singleton del motor DB + inicialización del esquema
│   │   ├── bronze_loader.py    <- Persistencia de datos crudos
│   │   └── gold_builder.py     <- Upsert del esquema estrella + calculo del KPI
│   ├── orchestration/
│   │   └── flows.py            <- Definiciones @flow + @task de Prefect
│   ├── pipeline/
│   │   └── main.py             <- Punto de entrada CLI (argumento --years)
│   └── utils/
│       └── config.py           <- URLs SNIES, config BD, contratos de columnas
│
├── data/
│   ├── raw/                    <- Archivos SNIES descargados (cache)
│   ├── processed/              <- (reservado para futuras exportaciones CSV)
│   └── gold/                   <- (reservado para futuras exportaciones Parquet)
│
├── Dockerfile                  <- Construcción multi-etapa con uv
├── docker-compose.yaml         <- PostgreSQL + pipeline + Prefect (opcional)
├── init.sql                    <- Bootstrap del esquema PostgreSQL
├── pyproject.toml              <- Manifiesto del proyecto uv
├── uv.lock                     <- Versiones fijadas de las dependencias
└── README.md
```

---

## Transferencia de Conocimiento: Escalabilidad a Nivel Nacional

> Esta sección responde a la pregunta: _"Como escalaría esta solución si tuviéramos que integrar datos de todo el país?"_

### Estado actual vs. Escala nacional

| Dimensión              | Actual (Bogotá, 3 años) | Nacional (todos los deptos, todos los años) |
| ---------------------- | ----------------------- | ------------------------------------------- |
| Instituciones          | ~100                    | ~400+                                       |
| Registros / archivo    | ~50K filas              | ~2M+ filas                                  |
| Archivos / año         | 2                       | 2                                           |
| Volumen total de datos | < 100 MB                | Varios GB por año                           |

A escala nacional, la arquitectura actual requiere las siguientes mejoras:

### 1. Almacenamiento: Parquet + Almacenamiento de objetos en lugar del sistema de archivos local

Reemplazar `data/raw/` con un almacenamiento de objetos compatible con S3 (AWS S3, Google Cloud Storage, o MinIO para entornos auto-hospedados). Almacenar archivos crudos como Parquet particionados por `(año, departamento)`:

```
s3://snies-data/bronze/docentes/year=2024/departamento=cundinamarca/*.parquet
```

Esto habilita el pushdown de predicados: las herramientas posteriores solo escanean las particiones que necesitan.

### 2. Procesamiento: Polars o PySpark en lugar de pandas

Para conjuntos de datos de varios millones de filas, `pandas` se convierte en un cuello de botella (un solo hilo, carga en memoria). Se recomienda migrar a:

- **Polars**: Reemplazo directo de pandas, 10-20 veces mas rápido, evaluacion perezosa (lazy).
- **PySpark**: Para conjuntos de datos que exceden la memoria RAM de una sola máquina (distribución a traves de un cluster).

La lógica de transformación en `processing/` esta diseñada como funciones puras: cambiar la librería de DataFrames solo requiere modificar las implementaciones de las funciones, no la estructura del pipeline.

### 3. Orquestacion: Airflow (gestionado) para programación a escala de equipo

Con un equipo de ingenieros y multiples dominios de datos, el modelo de servidor unico de Prefect resulta limitante. Se recomienda migrar a **Apache Airflow en Cloud Composer** (GCP) o **Amazon MWAA** (AWS):

- Control de acceso basado en roles para la propiedad de los DAGs.
- Monitoreo de SLA en varios equipos.
- Integracion nativa con almacenamiento y computo en la nube.

Cada `@task` de Prefect se mapea directamente a un `PythonOperator` de Airflow, haciendo que la migración sea mecánica.

### 4. Modelado de datos: partición de la capa Gold por `departamento`

Agregar `departamento` a `dim_periodo` y particionar `fact_capacidad_academica` por este mismo. El particionamiento nativo de PostgreSQL (o un almacenamiento en columnas como ClickHouse/BigQuery) mantiene la velocidad de consulta incluso a escala nacional.

### 5. Catálogo de datos y gobierno

A medida que crece el numero de conjuntos de datos y consumidores, se debe agregar:

- **OpenMetadata** o **DataHub** para un catálogo de datos que permita búsquedas.
- **dbt** para las transformaciones Silver -> Gold basadas en SQL, con documentación automatizada y linaje.
- **Great Expectations** para validaciones de calidad de datos (DQ) mas avanzadas que los controles actuales integrados en el código.

### 6. CI/CD para validación del pipeline

Agregar un flujo de trabajo de GitHub Actions que:

1. Ejecute `uv sync --frozen` (auditoria de dependencias).
2. Ejecute pruebas unitarias con `STRICT_DQ=true` contra un conjunto de datos de muestra.
3. Valide que la construcción de `docker compose build` se complete exitosamente en cada Pull Request.

```yaml
# .github/workflows/pipeline-ci.yml
on: [push, pull_request]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - run: uv sync --frozen
      - run: docker compose build pipeline
```
