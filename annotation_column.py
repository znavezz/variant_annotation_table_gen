import pandas as pd

class AnnotationColumn:
    def __init__(
        self,
        name: str,
        compute_function: callable,
        description: str | None = None,
        data_type: str | None = None
    ) -> None:
        """
        Initializes an AnnotationColumn object.

        :param name: Name of the annotation column.
        :param description: Description of the annotation column.
        :param data_type: Data type of the annotation column.
        """
        self.name = name
        self.compute_function = compute_function
        self.description = description
        self.data_type = data_type

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Computes the annotation for the given DataFrame.

        :param df: DataFrame to compute the annotation for.
        :return: Series of computed annotations.
        """
        return self.compute_function(df)