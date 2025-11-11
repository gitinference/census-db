from jp_tools import download
import duckdb
import geopandas as gpd
import tempfile
import logging
import os


class data_pull:
    def __init__(
        self,
        saving_dir: str = "data/",
        db_file: str = "sqlite:///database.db",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.db_file = db_file
        self.conn = duckdb.connect()
        self.conn.execute("LOAD sqlite;")
        self.conn.execute(f"ATTACH '{self.db_file[10:]}' AS sqlite_db (TYPE sqlite);")

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
