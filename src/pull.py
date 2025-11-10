from jp_tools import download
import geopandas as gpd
import tempfile
import logging
import os


class data_pull:
    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir

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
        if not os.path.exists(f"{self.saving_dir}external/geo-us-states.parquet"):
            download(
                url="https://www2.census.gov/geo/tiger/TIGER2025/STATE/tl_2025_us_state.zip",
                filename=f"{tempfile.gettempdir()}/geo-us-states.zip",
            )
            gdf = gpd.read_file(f"{tempfile.gettempdir()}/geo-us-states.zip")
            gdf.to_parquet(f"{self.saving_dir}external/geo-us-states.parquet")
        return gpd.read_parquet(f"{self.saving_dir}external/geo-us-states.parquet")
