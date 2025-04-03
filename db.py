import pandas as pd
from abc import ABC, abstractmethod

class Db(ABC):
    """
    Abstract base class for databases.
    """
    def __init__(
            self,
            name: str,
            key_cols: list[str],
            description: str | None = None,
            df: pd.DataFrame | None = None,
            rest_cols: list[str] | None = None,
            pre_processors: list[callable] | None = None,
    ) -> None:
        """
        Initializes a database object.

        :param name: Name of the database.
        :param description: Description of the database.
        :param df: DataFrame containing the database data.
        :param key_cols: List of key columns in the database.
        :param rest_cols: List of other columns in the database.
        :param pre_processors: List of pre-processing functions to apply to the DataFrame.
        """
        self.name = name
        self.description = description
        self.df = df if df is not None else pd.DataFrame()
        self.key_cols = key_cols
        self.rest_cols = rest_cols if rest_cols is not None else []
        self.pre_processors = pre_processors if pre_processors is not None else []

    def add_pre_processor(self, pre_processor: callable) -> None:
        """
        Adds a pre-processing function to the list of pre-processors.

        :param pre_processor: Pre-processing function to add.
        """
        self.pre_processors.append(pre_processor)
        print(f"Pre-processor '{pre_processor.__name__}' added to database '{self.name}'.")

    def pre_process(self) -> pd.DataFrame:
        """
        Applies all pre-processing functions to the DataFrame.
        """
        for pre_processor in self.pre_processors:
            self.df = pre_processor(self.df)
            print(f"Applied pre-processor '{pre_processor.__name__}' to database '{self.name}'.")
        return self.df

    def get_name(self) -> str:
        """
        Returns the name of the database.
        """
        return self.name
    
    def get_description(self) -> str | None:
        """
        Returns the description of the database.
        """
        return self.description
    


class VariantDb(Db):
    def pre_process(self):
        print(f"Pre-processing VariantDb: {self.name}")
        return super().pre_process()
    
class ValidationDb(Db):
    def pre_process(self):
        print(f"Pre-processing ValidationDb: {self.name}")
        return super().pre_process()