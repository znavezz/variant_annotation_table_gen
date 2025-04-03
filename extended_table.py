import pandas as pd
from annotation_column import AnnotationColumn
from db import Db, VariantDb, ValidationDb
from instructions_provider import InstructionsProvider


class ExtendedTable:
    def __init__(self, key_cols: list[str], instructions_provider: InstructionsProvider, ann_cols: list[AnnotationColumn] | None = None) -> None:
        self.table: pd.DataFrame = pd.DataFrame()
        self.variant_dbs: list[VariantDb] = []
        self.validation_dbs: list[ValidationDb] = []
        self.key_cols: list[str] = key_cols
        self.instructions_provider: InstructionsProvider = instructions_provider
        self.ann_cols: list[AnnotationColumn] = ann_cols if ann_cols is not None else []

    def upload_table(self, file_path: str) -> None:
        """
        Uploads a table from a file path.
        """
        self.table = pd.read_csv(file_path)
        print(f"Table uploaded from {file_path}.")

    def create_basic_table(self) -> None:
        """
        Creates a basic table with key columns.
        """
        self.table = pd.DataFrame(columns=self.key_cols)
        print("Basic table created with key columns.")

    def register_db(self, db: Db) -> None:
        """
        Adds a database to the list of databases.
        """
        if isinstance(db, VariantDb):
            self.variant_dbs.append(db)
            print(f"Variant database {db.name} added.")
        elif isinstance(db, ValidationDb):
            self.validation_dbs.append(db)
            print(f"Validation database {db.name} added.")
        else:
            raise ValueError("Unsupported database type.")
        
    def merge_db(self, db: Db) -> None:
        """
        Merges a VariantDb into the extended table by splitting its variants into:
        - Existing variants: those already in the extended table.
        - New variants: those not present in the extended table.
        
        For existing variants, only the indicator column for this db is updated to 1.
        For new variants, annotation columns are computed and new rows are appended.
        """
        if not isinstance(db, VariantDb):
            raise ValueError("Only VariantDb can be merged into the extended table.")
        
        # Pre-process the db to get a standardized DataFrame.
        self.instructions_provider.instructions["pre_processor"](db.df)
        
        # Define the indicator column name for this database.
        indicator_col: str = f"{db.name}_db"
        
        # Ensure the indicator column exists in the extended table.
        if indicator_col not in self.table.columns:
            self.table[indicator_col] = 0

        # If the extended table is empty, then all rows from db.df are new.
        if self.table.empty:
            new_df = db.df.copy()
            new_df[indicator_col] = 1
            # Compute annotation columns for the new variants.
            for ann in self.ann_cols:
                self.instructions_provider.instructions[ann.name]["compute_function"](new_df)
            self.table = new_df
            print(f"{len(new_df)} out of {len(db.df)} new variants added from {db.name}.")
            return

        # Use a left merge (with indicator) to identify which rows in db.df are already present.
        merge_df = pd.merge(
            db.df,
            self.table[self.key_cols],
            on=self.key_cols,
            how='left',
            indicator=True
        )
        
        # 'both' indicates rows already in self.table; 'left_only' are new variants.
        existing_df = merge_df[merge_df['_merge'] == 'both']
        new_df = merge_df[merge_df['_merge'] == 'left_only'].drop(columns=['_merge'])
        
        # Update the indicator column for existing variants.
        # Build a set of keys (tuples) for the existing variants.
        existing_keys = {tuple(row) for row in existing_df[self.key_cols].to_numpy()}
        # Create a boolean mask for self.table rows whose key is in the existing_keys set.
        mask = self.table[self.key_cols].apply(lambda row: tuple(row) in existing_keys, axis=1)
        self.table.loc[mask, indicator_col] = 1
        
        # For new variants, compute annotation values and set the indicator.
        new_df[indicator_col] = 1

        for ann in self.ann_cols:
            self.instructions_provider.instructions[ann.name]["compute_function"](new_df)
        
        # Align new_df with self.table (ensuring all expected columns are present).
        new_df = new_df.reindex(columns=self.table.columns, fill_value=0)
        
        # Append the new variants to the extended table.
        self.table = pd.concat([self.table, new_df], ignore_index=True)
        
        print(f"Database '{db.name}' merged: {len(existing_df)} existing variants updated and {len(new_df)} new variants added.")


    def register_annotation_column(self, annotation_col: AnnotationColumn) -> None:
        """
        Register an annotation column to the extended table.
        This method applies the compute_function for the new column across the entire table,
        and then stores the annotation column for consistency in future updates.
        """
        self.ann_cols.append(annotation_col)
        print(f"Annotation column '{annotation_col.name}' added.")

    def update_annotation_column(self, annotation_col: AnnotationColumn) -> None:
        """
        Updates an existing annotation column in the extended table.
        This method applies the compute_function for the updated column across the entire table.
        """
        if annotation_col in self.ann_cols:
            self.table[annotation_col.name] = self.table.apply(annotation_col.compute, axis=1)
            print(f"Annotation column '{annotation_col.name}' updated.")
        else:
            raise ValueError(f"Annotation column '{annotation_col.name}' not found in the table.")

    

    def save_table(self, file_path: str) -> None:
        """
        Saves the extended table to a file path.
        """
        self.table.to_csv(file_path, index=False)
        print(f"Table saved to {file_path}.")

    def validate_table(self) -> None:
        """
        Validates the extended table against registered validation databases.
        This method checks for any discrepancies or errors in the data.
        """
        for db in self.validation_dbs:
            db.validate(self.table)
            print(f"Table validated against {db.name}.")

    def get_table(self) -> pd.DataFrame:
        """
        Returns the current state of the extended table.
        """
        return self.table
    def __clear_table(self) -> None:
        """
        Clears the current state of the extended table.
        """
        self.table = pd.DataFrame()
        self.variant_dbs = []
        self.validation_dbs = []
        print("Extended table cleared.")