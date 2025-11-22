from .pull import data_pull
import polars as pl


class data_inserts(data_pull):
    def __init__(
        self,
        saving_dir: str = "data/",
        db_file: str = "sqlite:///database.db",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, db_file, log_file)

        self.data = self.pull_urls()

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

    def insert_urls(self):
        urls = self.data.unique(subset="dataset", maintain_order=True, keep="last")
        urls = urls.with_columns(
            api_url=pl.col("c_variablesLink")
            .str.slice(32)
            .str.replace("variables.json", ""),
        ).select("dataset", "api_url")

        urls.write_database(
            table_name="urls_table", if_table_exists="append", connection=self.db_file
        )
