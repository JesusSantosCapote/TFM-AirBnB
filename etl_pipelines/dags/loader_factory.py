from data_loaders import PostgresLoader, DataLoader

class LoaderFactory:
    @staticmethod
    def get_loader(loader_type: str) -> DataLoader:
        if loader_type == "postgres":
            return PostgresLoader()