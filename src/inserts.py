from .pull import data_pull
import polars as pl


class data_inserts(data_pull):
    def __init__(self, saving_dir: str = "data/", log_file: str = "data_process.log"):
        super().__init__(saving_dir, log_file)

    def insert_states(self) -> None:
        gdf = self.pull_states()
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
            connection="sqlite:///database.db",
        )
