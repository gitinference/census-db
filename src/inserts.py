from .pull import data_pull
import polars as pl
from sqlmodel import create_engine, Session
from .models import regions


class data_inserts(data_pull):
    def __init__(
        self,
        saving_dir: str = "data/",
        conn: str = "sqlite:///database.db",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, conn, log_file)
        self.engine = create_engine(self.conn)

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
                "STATEFP": "states_fips",
                "STUSPS": "name_abv",
                "NAME": "name_full",
            }
        )
        df.write_database(
            table_name="states",
            if_table_exists="append",
            connection=self.conn,
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
            table_name="regions", if_table_exists="append", connection=self.conn
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
            table_name="divisions", if_table_exists="append", connection=self.conn
        )
