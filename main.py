from src.inserts import data_inserts

di = data_inserts()


def main():
    # di.insert_regions()
    # di.insert_states()
    # di.insert_divisions()
    # di.insert_county()
    # di.insert_track()
    # di.insert_datasets()
    # di.insert_years()
    # di.insert_geo_full()
    di.insert_var_full()


if __name__ == "__main__":
    main()
