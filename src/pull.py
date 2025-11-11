from jp_tools import download
import geopandas as gpd
import tempfile
import logging
import os


class data_pull:
    def __init__(
        self,
        saving_dir: str = "data/",
        conn: str = "sqlite:///database.db",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.conn = conn

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename=log_file,
        )
        # Check if the saving directory exists
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
        if not os.path.exists(self.saving_dir + "external"):
            os.makedirs(self.saving_dir + "external")

    def pull_states(self) -> gpd.GeoDataFrame:
        filename = "geo-us-states.parquet"

        if not os.path.exists(f"{self.saving_dir}external/geo-us-states.parquet"):
            temp_filename = f"{tempfile.gettempdir()}/{hash(filename)}.zip"
            download(
                url="https://www2.census.gov/geo/tiger/TIGER2025/STATE/tl_2025_us_state.zip",
                filename=temp_filename,
            )
            gdf = gpd.read_file(temp_filename)
            gdf.to_parquet(f"{self.saving_dir}external/{filename}")
        return gpd.read_parquet(f"{self.saving_dir}external/{filename}")

    def pull_geos(self, url: str, filename: str) -> gpd.GeoDataFrame:
        if not os.path.exists(filename):
            temp_filename = f"{tempfile.gettempdir()}/{hash(filename)}.zip"
            download(
                url=url,
                filename=temp_filename,
            )
            gdf = gpd.read_file(temp_filename)
            gdf.to_parquet(filename)
        return gpd.read_parquet(filename)
