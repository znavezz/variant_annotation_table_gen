import pandas as pd
from abc import ABC, abstractmethod

class Db(ABC):
    """
    Abstract base class for databases.
    """
    def __init__(
            self,
            db_path: str,
            instructions: dict[str, any],

    ) -> None:
        """
        Initializes a database object.

        :param db_path: Path to the database.
        :param instructions: Instructions for the database.
        """
        self.db_path = db_path
        self.instructions = instructions
        self.name = instructions.get("name")
        self.key_cols = instructions.get("key_cols")
        self.description = instructions.get("description", None)
        self.pre_processor = instructions.get("pre_processor", None)
        self.df = None

    def pre_process(self) -> pd.DataFrame:
        """
        Pre-processes the DataFrame using the provided pre-processing functions.
        """
        if self.df is None:
            raise ValueError("DataFrame is not set.")
        
        if not callable(self.pre_processor):
            raise ValueError("Pre-processor is not callable.")
        self.df = self.pre_processor(self.df)
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
    
    def upload_db(self, file_path: str) -> None:
        """
        Uploads the database from a file path.
        """
        self.df = pd.read_csv(file_path)
        print(f"Database '{self.name}' uploaded from {file_path}.")

    


class VariantDb(Db):
    def __init__(self, db_path: str, instructions: dict[str, any]) -> None:
        """
        Initializes a VariantDb object.

        :param db_path: Path to the database.
        :param instructions: Instructions for the database.
        """
        super().__init__(db_path, instructions)
        self.df = None
        self.annotations = instructions.get("annotations", None)
    
    def get_annotations(self) -> dict[str, any]:
        """
        Returns the annotations for the database.
        """
        return self.annotations
    
    def set_annotations(self, annotations: dict[str, any]) -> None:
        """
        Sets the annotations for the database.
        """
        self.annotations = annotations
        
    
class ValidationDb(Db):
    def __init__(self, db_path: str, instructions: dict[str, any]) -> None:
        """
        Initializes a ValidationDb object.

        :param db_path: Path to the database.
        :param instructions: Instructions for the database.
        """
        super().__init__(db_path, instructions)
        self.df = None
        self.validator = instructions.get("validator", None)

    def get_validator(self) -> dict[str, any]:
        """
        Returns the validator for the database.
        """
        return self.validator
    
    def set_validator(self, validator: dict[str, any]) -> None:
        """
        Sets the validator for the database.
        """
        self.validator = validator