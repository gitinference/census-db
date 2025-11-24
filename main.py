from src.inserts import data_inserts

di = data_inserts()


def main():
    di.insert_geo_full()


if __name__ == "__main__":
    main()
