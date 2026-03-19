import logging
import os

import polars as pl
import requests
from dotenv import load_dotenv
from jp_tools import notify
from polars.exceptions import SchemaError

from .pull import data_pull


class data_inserts(data_pull):
    def __init__(
        self,
        saving_dir: str = "data/",
        db_file: str = "sqlite:///database.db",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, db_file, log_file)
        self.insert_geo_missing()
        self.insert_var_missing()
        load_dotenv()

        # self.data = self.pull_urls()
        # INFO: Need to add documentation for the code

    def insert_states(self) -> None:
        gdf = self.pull_geos(
            url="https://www2.census.gov/geo/tiger/TIGER2025/STATE/tl_2025_us_state.zip",
            filename=f"{self.saving_dir}external/geo-us-states.parquet",
        )
        df = pl.DataFrame(gdf[["REGION", "DIVISION", "STATEFP", "STUSPS", "NAME"]])
        df = df.rename(
            {
                "REGION": "region_id",
                "DIVISION": "division_id",
                "STATEFP": "id",
                "STUSPS": "name_abv",
                "NAME": "name_full",
            }
        )
        df.write_database(
            table_name="states",
            if_table_exists="append",
            connection=self.db_file,
        )

    def insert_county(self) -> None:
        gdf = self.pull_geos(
            url="https://www2.census.gov/geo/tiger/TIGER2025/COUNTY/tl_2025_us_county.zip",
            filename=f"{self.saving_dir}external/geo-us-county.parquet",
        )
        df = pl.DataFrame(gdf[["GEOID", "STATEFP", "NAME"]])
        df = df.rename({"STATEFP": "state_id", "GEOID": "id", "NAME": "name_full"})
        df.write_database(
            table_name="county",
            if_table_exists="append",
            connection=self.db_file,
        )

    def insert_track(self) -> None:
        states = (
            self.conn.execute("SELECT id FROM sqlite_db.states;").df()["id"].tolist()
        )
        for state in states:
            gdf = self.pull_geos(
                url=f"https://www2.census.gov/geo/tiger/TIGER2025/TRACT/tl_2025_{str(state).zfill(2)}_tract.zip",
                filename=f"{self.saving_dir}external/geo-us-{str(state).zfill(2)}-tack.parquet",
            )
            gdf["county_id"] = gdf["STATEFP"] + gdf["COUNTYFP"]
            df = pl.DataFrame(gdf[["GEOID", "county_id", "STATEFP", "NAME"]])
            df = df.rename({"GEOID": "id", "STATEFP": "state_id", "NAME": "name_full"})
            df.write_database(
                table_name="track",
                if_table_exists="append",
                connection=self.db_file,
            )

    def insert_regions(self) -> None:
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name_abv": ["nw", "mw", "s", "w"],
                "name_full": ["northwest", "midwest", "south", "west"],
            }
        )
        df.write_database(
            table_name="regions", if_table_exists="append", connection=self.db_file
        )

    def insert_divisions(self) -> None:
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "region_id": [1, 1, 2, 2, 3, 3, 3, 4, 4],
                "name_abv": [
                    "ne",
                    "ma",
                    "enc",
                    "wnc",
                    "sa",
                    "esc",
                    "wsc",
                    "mtn",
                    "pac",
                ],
                "name_full": [
                    "New England",
                    "Middle Atlantic",
                    "East North Central",
                    "West North Central",
                    "South Atlantic",
                    "East South Central",
                    "West South Central",
                    "Mountain",
                    "Pacific",
                ],
            }
        )
        df.write_database(
            table_name="divisions", if_table_exists="append", connection=self.db_file
        )

    def insert_datasets(self, notify_flag: bool = False) -> None:
        urls = self.pull_urls().unique(
            subset="dataset", maintain_order=True, keep="last"
        )
        urls = urls.with_columns(
            api_url=pl.col("c_variablesLink")
            .str.slice(32)
            .str.replace("variables.json", ""),
        ).select("dataset", "api_url")

        urls.write_database(
            table_name="dataset_table",
            if_table_exists="append",
            connection=self.db_file,
        )
        if notify_flag:
            notify(
                url=str(os.environ.get("URL")),
                auth=str(os.environ.get("AUTH")),
                msg="Successfully completed inserting Databases",
            )

    def insert_years(self) -> None:
        years = self.pull_urls().unique(
            subset="c_vintage", maintain_order=True, keep="last"
        )
        years = years.rename({"c_vintage": "year"})
        years = years.with_columns(pl.col("year").fill_null(0))
        years = years.select("year").sort("year")
        years.write_database(
            table_name="year_table", if_table_exists="append", connection=self.db_file
        )

    def insert_geo_interm(self, dataset_id: int, geo_id: int, year_id: int) -> None:
        self.conn.execute(
            """
            INSERT INTO sqlite_db.geo_interm
                (dataset_id, geo_id, year_id) VALUES (?,?,?);
            """,
            (dataset_id, geo_id, year_id),
        )

    def insert_geo_item(self, geo_desc: str, geo_lv: str) -> None:
        self.conn.execute(
            """
            INSERT INTO sqlite_db.geo_table
                (geo_name, geo_lv) VALUES (?,?);
            """,
            (geo_desc, geo_lv),
        )

    def insert_geo_missing(self) -> None:
        query = self.conn.execute("""
            SELECT * FROM sqlite_db.geo_table
                WHERE id=1;
        """).fetchone()
        if query is None:
            self.conn.execute("""
                INSERT INTO sqlite_db.geo_table
                    (id, geo_name, geo_lv) VALUES (1, 'missing','missing');
            """)

    def insert_var_missing(self) -> None:
        query = self.conn.execute("""
            SELECT * FROM sqlite_db.variable_table
                WHERE id=1;
            """).fetchone()
        if query is None:
            self.conn.execute("""
                INSERT INTO sqlite_db.variable_table
                    (id, var_name, var_label) VALUES (1, 'missing','missing');
                """)

    def insert_geo_full(self, notify_flag: bool = False) -> None:
        # Loop to go through all available years and get their geografies
        for url in self.pull_urls().select("c_geographyLink").to_series().to_list():
            # special case for timeseries data
            if url.split("/")[4] == "timeseries":
                year_id = self.get_year_id(0)
                dataset_id = self.get_dataset_id(
                    "timeseries-" + "-".join(url.split("/")[5:-1])
                )

            else:
                year_id = self.get_year_id(url.split("/")[4])
                dataset_id = self.get_dataset_id("-".join(url.split("/")[5:-1]))

            if self.check_geo_interm_id(dataset_id=dataset_id, year_id=year_id):
                logging.debug(
                    f"Skipping there is data in geo_iterm table for {year_id} {dataset_id}"
                )
                continue

            # get all fips code for the available states
            results = requests.get(url).json().get("fips")

            try:
                df = pl.DataFrame(results)
            except SchemaError:
                clean = [
                    {k: v for k, v in rec.items() if k != "wildcard"} for rec in results
                ]
                df = pl.DataFrame(clean)

            if df.is_empty():
                self.insert_geo_interm(dataset_id=dataset_id, year_id=year_id, geo_id=1)
                logging.debug("inserted empty data")
                continue

            # registering of missing data & search from drataframe to get the needed data
            if "geoLevelId" not in df.columns and "geoLevelDisplay" not in df.columns:
                self.insert_geo_interm(dataset_id=dataset_id, year_id=year_id, geo_id=1)
                logging.debug("insert mising data")
                continue

            if "geoLevelId" in df.columns:
                df = df.with_columns(geoLevelDisplay=pl.col("geoLevelId"))

            df = df.with_columns(pl.col("geoLevelDisplay").fill_null("-1"))
            for obs in (
                df.select(pl.col("name") + "_" + pl.col("geoLevelDisplay"))
                .to_series()
                .to_list()
            ):
                geo_desc, geo_lv = obs.split("_")

                if geo_lv == "-1":
                    geo_lv = self.get_geo_desc(geo_name=geo_desc)

                if self.get_geo_id(geo_lv=geo_lv) == -1:
                    self.insert_geo_item(geo_desc=geo_desc, geo_lv=geo_lv)
                    logging.debug(f"inserted {geo_desc} into db")

                geo_id = self.get_geo_id(geo_lv=geo_lv)

                if self.check_geo_interm_id(
                    dataset_id=dataset_id, geo_id=geo_id, year_id=year_id
                ):
                    logging.debug(
                        f"Entry {dataset_id}  {geo_id} {year_id} is in dataset"
                    )
                    continue
                else:
                    self.insert_geo_interm(
                        dataset_id=dataset_id, geo_id=geo_id, year_id=year_id
                    )
                    logging.debug(
                        f"inserted {dataset_id}  {geo_id} {year_id} succesfully"
                    )

        if notify_flag:
            notify(
                url=str(os.environ.get("URL")),
                auth=str(os.environ.get("AUTH")),
                msg="Successfully completed inserting intermediate Geografies",
            )

    def insert_variable_interm(
        self, dataset_id: int, var_id: int, year_id: int
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO sqlite_db.variable_interm
                (dataset_id, var_id, year_id) VALUES (?,?,?);
            """,
            (dataset_id, var_id, year_id),
        )

    def insert_var_item(self, var_name: str, var_label: str) -> None:
        self.conn.execute(
            """
            INSERT INTO sqlite_db.variable_table
                (var_name, var_label) VALUES (?,?);
            """,
            (var_name, var_label),
        )

    def insert_var_full(self, notify_flag: bool = False) -> None:
        for url in self.pull_urls().select("c_variablesLink").to_series().to_list():
            if url.split("/")[4] == "timeseries":
                year_id = self.get_year_id(0)
                dataset_id = self.get_dataset_id(
                    "timeseries-" + "-".join(url.split("/")[5:-1])
                )

            else:
                year_id = self.get_year_id(url.split("/")[4])
                dataset_id = self.get_dataset_id("-".join(url.split("/")[5:-1]))

            if self.check_variable_interm_id(dataset_id=dataset_id, year_id=year_id):
                logging.debug(
                    f"Skipping there is data in var_iterm table for {year_id} {dataset_id}"
                )
                continue

            logging.debug(url)
            results = requests.get(url).json().get("variables")

            df = pl.DataFrame([{"name": k, **v} for k, v in results.items()])

            if df.is_empty():
                self.insert_variable_interm(
                    dataset_id=dataset_id, year_id=year_id, var_id=1
                )
                logging.debug("inserted empty data")
                continue

            df = df.select(
                var_name=pl.col("name"),
                var_label=pl.col("label"),
                dataset_id=dataset_id,
                year_id=year_id,
            )

            # insert variables not in DB
            in_db = self.conn.execute("""
                SELECT var_name FROM sqlite_db.variable_table;
                """).pl()
            db_list = in_db.select(pl.col("var_name")).to_series().to_list()
            candidate = df.select(pl.col("var_name")).to_series().to_list()
            insert_db = list(set(candidate) - set(db_list))

            if len(insert_db) > 0:
                data = df.filter(pl.col("var_name").is_in(insert_db))
                data = data.select(pl.col("var_name", "var_label"))
                data.write_database(
                    table_name="variable_table",
                    if_table_exists="append",
                    connection=self.db_file,
                )
                logging.debug(f"Inserted {len(insert_db)} succesfully")

            # insert into intermediate table
            in_db = self.conn.execute("""
                SELECT id,var_name FROM sqlite_db.variable_table;
                """).pl().rename({"id": "var_id"})
            df = df.join(in_db, on=["var_name"], how="left", validate="1:1")
            df = df.select(["dataset_id", "year_id", "var_id"])
            df.write_database(
                table_name="variable_interm",
                if_table_exists="append",
                connection=self.db_file,
            )
            logging.debug(f"inserted {len(df)} succesfully")

        if notify_flag:
            notify(
                url=str(os.environ.get("URL")),
                auth=str(os.environ.get("AUTH")),
                msg="Successfully completed inserting intermediate Geografies",
            )

    def get_year_id(self, year: int) -> int:
        quary = self.conn.execute(
            """
            SELECT *
                FROM sqlite_db.year_table
                WHERE year = ?;
        """,
            (year,),
        ).fetchone()
        if quary is None:
            raise ValueError(f"No entry found for year {year}")
        return int(quary[0])

    def get_dataset_id(self, dataset: str) -> int:
        query = self.conn.execute(
            """
            SELECT *
                FROM sqlite_db.dataset_table
                WHERE dataset = ?;
        """,
            (dataset,),
        ).fetchone()
        if query is None:
            raise ValueError(f"No entry found for dataset {dataset}")
        return int(query[0])

    def get_geo_id(self, geo_lv: str) -> int:
        query = self.conn.execute(
            """
            SELECT *
                FROM sqlite_db.geo_table
                WHERE geo_lv = ?;
        """,
            (geo_lv,),
        ).fetchone()
        if query is None:
            return -1
        return int(query[0])

    def get_geo_desc(self, geo_name: str) -> str:
        query = self.conn.execute(f"""
            SELECT *
                FROM sqlite_db.geo_table
                WHERE geo_name = '{geo_name}';
        """).fetchone()
        if query is None:
            raise ValueError(f"NO entry with that value {geo_name}")
        return query[2]

    def get_var_id(self, var_name: str) -> int:
        query = self.conn.execute(
            """
            SELECT *
                FROM sqlite_db.variable_table
                WHERE var_name = (?);
        """,
            (var_name,),
        ).fetchone()
        if query is None:
            return -1
        return int(query[0])

    def check_geo_interm_id(
        self, dataset_id: int, year_id: int, geo_id: int = -1
    ) -> bool:
        if geo_id == -1:
            query = self.conn.execute(
                """
            SELECT *
                FROM sqlite_db.geo_interm
                WHERE dataset_id=? AND year_id=?;
            """,
                (dataset_id, year_id),
            ).fetchone()
        else:
            query = self.conn.execute(
                """
            SELECT *
                FROM sqlite_db.geo_interm
                WHERE dataset_id=? AND geo_id=? AND year_id=?;
            """,
                (
                    dataset_id,
                    geo_id,
                    year_id,
                ),
            ).fetchone()
        if query is None:
            return False
        else:
            return True

    def check_variable_interm_id(
        self, dataset_id: int, year_id: int, var_id: int = -1
    ) -> bool:
        if var_id == -1:
            query = self.conn.execute(
                """
            SELECT *
                FROM sqlite_db.variable_interm
                WHERE dataset_id=? AND year_id=?;
            """,
                (dataset_id, year_id),
            ).fetchone()
        else:
            query = self.conn.execute(
                """
            SELECT *
                FROM sqlite_db.variable_interm
                WHERE dataset_id=? AND var_id=? AND year_id=?;
            """,
                (dataset_id, var_id, year_id),
            ).fetchone()
        if query is None:
            return False
        else:
            return True
