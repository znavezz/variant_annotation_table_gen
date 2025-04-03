import pandas as pd
from DBs.default.instructions import instructions
import importlib
import os


class InstructionsProvider:
    def __init__(self, instructions_path: str):
        self.instructions = None
        self.set_db_instructions("default")
        

    def get_instructions(self):
        return self.instructions
    
    def set_instructions(self, instructions):
        self.instructions = instructions
    
    def set_db_instructions(self, db):
        try:
            # Construct the module path dynamically
            module_path = f"DBs.{db}.instructions"
            
            # Dynamically import the module
            instructions_module = importlib.import_module(module_path)
            
            # Check if the module has the 'instructions' attribute
            if hasattr(instructions_module, 'instructions'):
                self.instructions = getattr(instructions_module, 'instructions')
                print(f"Successfully loaded instructions from DBs/{db}/instructions.py")
            else:
                print(f"No 'instructions' dictionary found in DBs/{db}/instructions.py")
        
        except ModuleNotFoundError:
            print(f"Error: The instructions file for '{db}' was not found.")
        except Exception as e:
            print(f"An error occurred while loading instructions: {e}")

    
    def get_annotations_names(self):
        return list(self.instructions["annotations"].keys())
    

if __name__ == "__main__":
    # Example usage
    instruction_provider = InstructionProvider(instructions)
    print(instruction_provider.get_instructions())
    
    # Example DataFrame
    data = pd.DataFrame({
        "ref": ["G", "A", "T"],
        "alt": ["A", "C", "G"]
    })
    
    # Apply pre-processing
    processed_data = instructions_provider.instructions["pre_processor"](data)

    annotations = instructions_provider.instructions["annotations"]
    
    # Apply annotation functions
    annotations["isADARFixable"]["compute_function"](processed_data)
    
    print(processed_data)