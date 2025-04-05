import pandas as pd
import importlib
import os
import sys
from db import Dbß


class InstructionsProvider:
    def __init__(self, dbs_path: str):
        sys.path.append(dbs_path)
        self.dbs_path, self.variants_dbs_path, self.validation_dbs_path = check_dbs_dir_structure(dbs_path)
        self.variants_instructions_map = self.get_db_instructions("default")
        self.validation_instructions_map = self.get_db_instructions("default", "validation")
        
    def get_variants_instructions_map(self):
        return self.variants_instructions_map
    
    def set_variants_instructions_map(self, instructions):
        self.variants_instructions_map = instructions
    
    def set_validation_instructions_map(self, instructions):
        self.validation_instructions_map = instructions
    
    def get_db_instructions(self, db_name: str, db_type: str = "variants"):
        """
        Loads the instructions from the specified database.
        Returns the instructions as a dictionary.
        """
        try:
            # Construct the path to the instructions module.
            instructions_path = os.path.join(self.dbs_path, db_type, db_name, "instructions.py")
            print(f"Loading instructions from {instructions_path}")
            if not os.path.isfile(instructions_path):
                raise FileNotFoundError(f"Instructions file not found at {instructions_path}.")
            # Add the directory to the system path.
            sys.path.append(os.path.dirname(instructions_path))
            # Import the instructions module.
            module_name = os.path.basename(instructions_path).replace(".py", "")
            print(f"Importing module '{module_name}' from path '{os.path.dirname(instructions_path)}'")
            print(f"module_name: {module_name}")
            instructions_module = importlib.import_module(module_name)
            # Get the instructions from the module.
            print(f"Getting instructions from module '{module_name}'")
            instructions = getattr(instructions_module, "default_instructions", None)
            print(f"Instructions loaded: {instructions}")
            if instructions is None:
                raise ImportError(f"Instructions not found in module '{module_name}'.")
            # Remove the directory from the system path.
            sys.path.remove(os.path.dirname(instructions_path))
            # Check if the instructions are a dictionary.
            if not isinstance(instructions, dict):
                raise TypeError(f"Instructions for '{db_name}' must be a dictionary.")
            
            return instructions
        except ModuleNotFoundError as e:
            print(f"")
            raise ImportError(f"Failed to import instructions module: {e}")
        
    def set_variants_db_instructions(self, db_name: str) -> dict:
        """
        Sets the instructions for the specified database.
        Loads the instructions from {self.dbs_path}/{db_name}/instructions.py.
        For the nested 'annotations' dict, it merges the default annotations with the db-specific annotations,
        using the db-specific ones when available and defaulting to those missing.
        For 'pre_processor', if it's missing in the db instructions, it uses the default pre_processor.
        """
        # Load the instructions for the specified db.
        try:
            db_instructions = self.get_db_instructions(db_name)
            # Load the default instructions to fill in missing values.
            default_instructions = self.get_db_instructions("default")
            if default_instructions is None:
                raise ValueError("Default instructions not found.")
            
            # Merge annotations:
            # Start with a copy of the default annotations.
            merged_annotations = default_instructions.get("annotations").copy()
            # If the db instructions provide annotations, update (override/add) them.
            if "annotations" in db_instructions and isinstance(db_instructions["annotations"], dict):
                merged_annotations.update(db_instructions["annotations"])
            
            # For the pre_processor, use the db instructions value if available, otherwise fall back to default.
            pre_processor = db_instructions.get("pre_processor", None)
            if pre_processor is None:
                pre_processor = default_instructions.get("pre_processor")
            
            # Build the merged instructions dictionary.
            merged_instructions = {
                "annotations": merged_annotations,
                "pre_processor": pre_processor,
            }
            
            # Update the variants_instructions_map.
            self.variants_instructions_map = merged_instructions
            print(f"Instructions for '{db_name}' loaded and merged with default where necessary.")
            return merged_instructions
        except FileNotFoundError as e:
            db_instructions = None
            print(f"Did not find instructions for database '{db_name}': {e}")
        except ImportError as e:
            db_instructions = None
            print(f"Failed to import instructions for database '{db_name}': {e}")
        except Exception as e:
            db_instructions = None
            print(f"An unexpected error occurred while loading instructions for database '{db_name}': {e}")
        finally:
            if db_instructions is None:
                db_instructions = self.get_db_instructions("default")
                print(f"Using default instructions instead.")

    
    def set_validation_db_instructions(self, db_name: str) -> dict:
        """
        Sets the instructions for the specified validation database.
        Loads the instructions from {self.dbs_path}/validation/{db_name}/instructions.py.
        The instructions file of the validation database should follow the same structure as the dont have annotations but have pre_process and validator functions.
        """
        try:
            db_instructions = self.get_db_instructions(db_name, "validation")
            # Load the default instructions to fill in missing values.
            default_instructions = self.get_db_instructions("default", "validation")
            if default_instructions is None:
                raise ValueError("Default validation instructions not found.")
            
            # For the pre_processor, use the db instructions value if available, otherwise fall back to default.
            pre_processor = db_instructions.get("pre_processor", None)
            if pre_processor is None:
                pre_processor = default_instructions.get("pre_processor")
            validator = db_instructions.get("validator", None)
            if validator is None:
                validator = default_instructions.get("validator")
            
            # Build the merged instructions dictionary.
            merged_instructions = {
                "pre_processor": pre_processor,
                "validator": validator,
            }
            
            # Update the validation_instructions_map.
            self.validation_instructions_map = merged_instructions
            print(f"Validation instructions for '{db_name}' loaded and merged with default where necessary.")
            return merged_instructions
        except FileNotFoundError as e:
            db_instructions = None
            print(f"Did not find validation instructions for database '{db_name}': {e}")
        except ImportError as e:
            db_instructions = None
            print(f"Failed to import validation instructions for database '{db_name}': {e}")
        except Exception as e:
            db_instructions = None
            print(f"An unexpected error occurred while loading validation instructions for database '{db_name}': {e}")
        finally:
            if db_instructions is None:
                db_instructions = self.get_db_instructions("default", "validation")
                print(f"Using default validation instructions instead.")
    def get_annotations_names(self):
        return list(self.variants_instructions_map["annotations"].keys())
    
    def get_dbs_names(self, db_type: str = "variants"):
        """
        Returns a list of database names in the specified directory.
        """
        dbs_path = self.variants_dbs_path if db_type == "variants" else self.validation_dbs_path
        if not os.path.isdir(dbs_path):
            raise NotADirectoryError(f"Path '{dbs_path}' is not a directory.")
        
        dbs_names = [name for name in os.listdir(dbs_path) if os.path.isdir(os.path.join(dbs_path, name)) and name != "default" and not name.startswith("__")] 
        return dbs_names
    
    def get_key_columns(self):
        """
        Returns the key columns for the database.
        """
        key_cols = self.variants_instructions_map.get("key_cols")
        if key_cols is None:
            raise ValueError("Key columns not found in the instructions.")
        return key_cols

    def create_db_instance(self, db_name: str, db_type: str = "variants"):
        """
        Creates a Db instance for the specified database.
        """
        try:
            if db_type == "variants":
                db_path = self.variants_dbs_path
            elif db_type == "validation":
                db_path = self.validation_dbs_path
            else:
                raise ValueError(f"Invalid db_type '{db_type}'. Must be 'variants' or 'validation'.")
            db_path = os.path.join(db_path, db_name)
            if not os.path.isdir(db_path):
                raise NotADirectoryError(f"Path '{db_path}' is not a directory.")
            
            # Load the instructions for the specified db.
            instructions_map = self.set_variants_db_instructions(db_name) if db_type == "variants" else self.set_validation_db_instructions(db_name)
            # Create a Db instance.
            db_key_cols = instructions_map("key_cols")
            db_description = instructions_map("description")
            db_pre_processor = instructions_map("pre_processor")
            db_instance = Db(
                name=db_name,
                key_cols=db_key_cols,
                db_path=db_path,
                description=db_description,
                pre_processor=db_pre_processor
            )

            return db_instance
        finally:
            # Reset the instructions to default after creating the instance.
            self.set_variants_db_instructions("default")


if __name__ == "__main__":
    dbs_path = "/Users/znave/Desktop/DBs"
    instructions_provider = InstructionsProvider(dbs_path)
    # Example usage
    print(instructions_provider.get_variants_instructions_map())
    
    # Example DataFrame
    data = pd.DataFrame({
        "ref": ["G", "A", "T", "T"],
        "alt": ["A", "C", "G", "C"]
    })
    instructions_provider.set_variants_db_instructions("default")
    
    # Apply pre-processing
    processed_data = instructions_provider.variants_instructions_map["pre_processor"](data)

    annotations = instructions_provider.variants_instructions_map["annotations"]
    print(f"Instructions map: {instructions_provider.get_variants_instructions_map()}")
    # Apply annotation functions
    annotations["isADARFixable"]["compute_function"](processed_data)
    annotations["isApoBecFixable"]["compute_function"](processed_data)
    
    print(processed_data)



def check_dbs_dir_structure(dbs_path: str):
    """
    Checks if the dbs_path contains the expected directory structure.
    The expected structure is:
    - dbs_path
        - variants
            - default
                - instructions.py
            - <other_db>
                - instructions.py
        - validation (optional, but if exist nust folow this format)
            - default
                - instructions.py
            - <other_db>
                - instructions.py
            
    """
    if not os.path.isdir(dbs_path):
        raise NotADirectoryError(f"Path '{dbs_path}' is not a directory.")
    
    variants_path = os.path.join(dbs_path, "variants")
    if not os.path.isdir(variants_path):
        raise NotADirectoryError(f"Path '{variants_path}' is not a directory.")
    default_instructions_path = os.path.join(variants_path, "default", "instructions.py")
    if not os.path.isfile(default_instructions_path):
        raise FileNotFoundError(f"Default instructions file not found at {default_instructions_path}.")
            
    validation_path = os.path.join(dbs_path, "validation")
    if os.path.isdir(validation_path):
        default_validation_instructions_path = os.path.join(validation_path, "default", "instructions.py")
        if not os.path.isfile(default_validation_instructions_path):
            raise FileNotFoundError(f"Default validation instructions file not found at {default_validation_instructions_path}.")
    else:
        validation_path = None
    return (dbs_path, variants_path, validation_path)