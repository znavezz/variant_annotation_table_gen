from instructions_provider import InstructionsProvider
from DBs.default.instructions import instructions
import extended_table
import os
import sys

def get_key_columns():
    """
    Prompts the user to enter key columns for the table.
    Returns a list of key column names.
    """
    usr_input = input("Please enter the key columns for the table (space-separated): ")
    return usr_input.split()

def main():
    instructions_provider = InstructionsProvider(instructions)
    key_cols = get_key_columns()
    print(f"Key columns for the new table: {key_cols}")
    ann_cols = instructions_provider.get_annotations_names()
    print(f"Available annotation columns: {ann_cols}")
    extended_table = extended_table.ExtendedTable(key_cols=key_cols, 
    instructions_provider=instructions_provider, ann_cols=ann_cols)
    ### TODO: complete the extended_table creation, and decide on structure fro where the db dirs will sit
    usr_input = input("If you have an existing variant table, please enter the path to it. If not, press Enter to continue: ")
    if usr_input:
        # Check if the file exists
        if os.path.isfile(usr_input):
            file_path = os.path.abspath(usr_input)
            print(f"File {usr_input} exists.")
            
            # Upload the existing table
            extended_table.upload_table(file_path)

        else:
            print(f"File {usr_input} does not exist. Please check the path and try again.")
    else:
        print("No file path provided. Proceeding without an existing variant table...")
        # Proceed with creating a new table
        extended_table.create_basic_table()

    # Register the databases

    

if __name__ == "__main__":
    main()