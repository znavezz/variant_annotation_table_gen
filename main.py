from instructions_provider import InstructionsProvider
from extended_table import ExtendedTable
import os
import sys


def get_dbs_dir():
    """
    Prompts the user to enter the path to the databases directory.
    Returns the path as a string.
    """
    usr_input = input("Please enter the path to the databases directory: ")
    if not os.path.isdir(usr_input):
        print(f"Directory \"{usr_input}\" does not exist. Please check the path and try again.")
        sys.exit(1)
    return usr_input



def main():
    dbs_dir = input("Please enter the path to the databases directory: ")
    while (not os.path.isdir(dbs_dir)):
        print(f"Directory \"{dbs_dir}\" does not exist. Please check the path and try again or press 'X' to exit.")
        usr_input = input()
        if usr_input.lower() == 'x':
            sys.exit(0)
        dbs_dir = usr_input
    sys.path.append(dbs_dir)

    instructions_provider = InstructionsProvider(dbs_dir)




    # itereate over the directories in the dbs_dir
    variants_dbs = instructions_provider.get_dbs_names()
    print(f"Databases found in the directory: {variants_dbs}")
    validation_dbs = instructions_provider.get_dbs_names("validation")
    print(f"Validation databases found in the directory: {validation_dbs}")

    annotations_cols = instructions_provider.get_annotations_names()
    print(f"Available annotation columns: {annotations_cols}")
    key_cols = instructions_provider.get_key_columns()
    print(f"Key columns for the new table: {key_cols}")
    for var_db in variants_dbs:
        # Create a Db instance for each database
        db_instance = instructions_provider.create_db_instance(var_db)
        print(f"Database instance created for {var_db}.")
    for val_db in validation_dbs:
        # Create a Db instance for each validation database
        db_instance = instructions_provider.create_db_instance(val_db, db_type="validation")
        print(f"Validation database instance created for {val_db}.")





    # default_instructions = os.path.join(dbs_dir, "default", "instructions.py")
    # if not os.path.isfile(default_instructions):
    #     print(f"Default instructions file not found at {default_instructions}. Please check the path and try again.")
    #     sys.exit(1)
    # # Load the default instructions
    
    # instructions = get_db_instructions(default_instructions)

    # instructions_provider = InstructionsProvider(instructions)
    # key_cols = get_key_columns()
    # print(f"Key columns for the new table: {key_cols}")
    # ann_cols = instructions_provider.get_annotations_names()
    # print(f"Available annotation columns: {ann_cols}")
    # extended_table = extended_table.ExtendedTable(key_cols=key_cols, 
    # instructions_provider=instructions_provider, ann_cols=ann_cols)
    # ### TODO: complete the extended_table creation
    # usr_input = input("If you have an existing variant table, please enter the path to it. If not, press Enter to continue: ")
    # if usr_input:
    #     # Check if the file exists
    #     if os.path.isfile(usr_input):
    #         file_path = os.path.abspath(usr_input)
    #         print(f"File {usr_input} exists.")
            
    #         # Upload the existing table
    #         extended_table.upload_table(file_path)

    #     else:
    #         print(f"File {usr_input} does not exist. Please check the path and try again.")
    # else:
    #     print("No file path provided. Proceeding without an existing variant table...")
    #     # Proceed with creating a new table
    #     extended_table.create_basic_table()

    # # Register the databases

    

if __name__ == "__main__":
    main()