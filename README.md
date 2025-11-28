# Census API → SQLite Mapping

_A complete system for harvesting, normalizing, and storing U.S. Census API metadata & geographies into a relational SQLite database.This has the purpose 
of creating a sql database for faster quary building_

---

## Project Overview

This project automates the discovery, download, normalization, and storage of **Census API datasets**, **variables**, and **geographic hierarchies** into a structured **SQLite database**.

It:

- Pulls dataset metadata from the U.S. Census API
- Builds lookup tables for:
  - Regions
  - Divisions
  - States
  - Counties
  - Census Tracts

- Inserts API-linked metadata:
  - Datasets
  - Years
  - Variables
  - Geography levels

- Manages intermediate relationship tables mapping datasets ↔ variables ↔ geographies ↔ years
- Cleans missing or malformed Census API responses
- Stores everything in a relational schema so quary builds becomes straightforward

The core of the system is the `data_inserts` class, which extends a base `data_pull` class and orchestrates the entire data ingestion pipeline.

---

## What the `data_inserts` Class Does

The class automates nearly every required step:

### Handles Missing Defaults

On initialization:

- Ensures **missing geography** record exists
- Ensures **missing variable** record exists
- Loads dataset + metadata URLs via `pull_urls()`

---

## Data Ingestion Components

### **1. Geographic Insert Operations**

| Method               | Description                                                                                                                             |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `insert_states()`    | Loads TIGER/Line state shapefile → SQLite table `states`                                                                                |
| `insert_county()`    | Loads county shapefile → `county`                                                                                                       |
| `insert_track()`     | Loops every state tract file → `track`                                                                                                  |
| `insert_regions()`   | Inserts static Census regions                                                                                                           |
| `insert_divisions()` | Inserts static Census divisions                                                                                                         |
| `insert_geo_full()`  | Parses API geography responses and populates: <br>• `geo_table` (geographic levels) <br>• `geo_interm` (dataset-year-geo relationships) |

The geo insertion logic forms the core feature of this project:
**discovering all possible API geographies and normalizing them**.

---

### **2. Dataset + Year Insert Operations**

| Method              | Description                                                |
| ------------------- | ---------------------------------------------------------- |
| `insert_datasets()` | Extracts dataset names + API paths from metadata           |
| `insert_years()`    | Extracts years (`c_vintage`) and inserts into `year_table` |

---

### **3. Variable Insert Operations**

| Method              | Description                                                                                                                                            |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `insert_var_full()` | Retrieves each dataset’s variable list, cleans it, and populates: <br>• `variable_table` <br>• `variable_interm` (dataset-year-variable relationships) |

Handles:

- Wildcards
- Missing labels
- Raw JSON to structured table

---

## Utility Methods

The class provides helper operations that support the ingestion workflow:

- `get_year_id(year)`
- `get_dataset_id(dataset)`
- `get_geo_id(geo_lv)`
- `get_geo_desc(geo_name)`
- `get_var_id(var_name)`
- Relationship-table checkers:
  - `check_geo_interm_id(...)`
  - `check_variable_interm_id(...)`

These ensure:

- Referential integrity
- No duplicate entries
- Late discovery of unknown variables or geographies is handled smoothly

---

# How to Use

## 1. Install Dependencies

```bash
pip install polars requests duckdb geopandas alembic
```
or using `uv`

```bash
uv sync
```

Aditionionaly you'll need to run the migration to prepare the database for the insertion of the data.

```bash
alembic upgrade head
```

---

## 2. Create and Run the Ingestion Pipeline

```python
from src.inserts import data_inserts

runner = data_inserts(
    saving_dir="data/",
    db_file="sqlite:///database.db",
    log_file="data_process.log",
)

runner.insert_regions()
runner.insert_divisions()
runner.insert_states()
runner.insert_county()
runner.insert_track()
runner.insert_datasets()
runner.insert_years()

runner.insert_geo_full()
runner.insert_var_full()
```

The class `data_insert` also includes a methode `self.conn` this methode can be used to quary directly form the database. Here is an example:

```python
from src.insert import data_inserts

di = data_inserts()

di.conn.execute(
    """
    SELECT * FROM sqlite_db.geo_table;
    """
).df()

```

This will return a `pd.DataFrame`