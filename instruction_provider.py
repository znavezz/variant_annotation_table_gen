import pandas as pd
import importlib
import os
import sys


class InstructionsProvider:
    def __init__(self, dbs_path: str):
        self.dbs_path = dbs_path
        sys.path.append(os.path.dirname(self.dbs_path))
        default_instructions_path = os.path.join(self.dbs_path, "default", "instructions.py")
        if not os.path.isfile(default_instructions_path):
            raise FileNotFoundError(f"Default instructions file not found at {default_instructions_path}.")
        self.instructions_map = self.get_db_instructions("default")
        

    def get_instructions_map(self):
        return self.instructions_map
    
    def set_instructions_map(self, instructions):
        self.instructions_map = instructions
    
    def get_db_instructions(self, db_name: str):
        """
        Loads the instructions from the specified database.
        Returns the instructions as a dictionary.
        """
        try:
            db_instructions_path = os.path.join(self.dbs_path, db_name, "instructions.py")
            if not os.path.isfile(db_instructions_path):
                raise FileNotFoundError(f"Instructions file not found at {db_instructions_path}")
                
            module_name = f"{os.path.basename(self.dbs_path)}.{db_name}.instructions"
            instructions_module = importlib.import_module(module_name)
            instructions = getattr(instructions_module, 'instructions', None)
            return instructions
        except ModuleNotFoundError as e:
            raise ImportError(f"Failed to import instructions module: {e}")
        
    def set_db_instructions(self, db_name: str):
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
        
        # Update the instructions_map.
        self.instructions_map = merged_instructions
        print(f"Instructions for '{db_name}' loaded and merged with default where necessary.")
    
    def get_annotations_names(self):
        return list(self.instructions_map["annotations"].keys())
    

if __name__ == "__main__":
    dbs_path = "/Users/znave/Desktop/DBs"
    instructions_provider = InstructionsProvider(dbs_path)
    # Example usage
    print(instructions_provider.get_instructions_map())
    
    # Example DataFrame
    data = pd.DataFrame({
        "ref": ["G", "A", "T"],
        "alt": ["A", "C", "G"]
    })
    instructions_provider.set_db_instructions("MSSNG")
    
    # Apply pre-processing
    processed_data = instructions_provider.instructions_map["pre_processor"](data)

    annotations = instructions_provider.instructions_map["annotations"]
    print(f"Instructions map: {instructions_provider.get_instructions_map()}")
    # Apply annotation functions
    annotations["isADARFixable"]["compute_function"](processed_data)
    
    print(processed_data)