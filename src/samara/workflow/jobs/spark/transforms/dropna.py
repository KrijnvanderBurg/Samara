"""Drop null values transform - Remove rows with missing data.

This module provides a transform function for dropping rows with null values
from a DataFrame. It enables data cleaning operations in the pipeline,
allowing you to handle missing data according to your requirements.

The DropNaFunction is registered with the TransformFunctionRegistry under the name
'dropna', making it available for use in configuration files and pipeline definitions.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame

from samara.workflow.jobs.models.transforms.model_dropna import DropNaFunctionModel
from samara.workflow.jobs.spark.transforms.base import FunctionSpark


class DropNaFunction(DropNaFunctionModel, FunctionSpark):
    """Drop rows with null values from a DataFrame.

    This transform enables removing rows that contain missing values from a DataFrame.
    Use it to clean data by removing incomplete records according to specified criteria.

    Attributes:
        function_type: The name of the function (always "dropna")
        arguments: Container for the drop null values parameters including how,
            thresh, and subset options

    Example:
        >>> dropna_fn = DropNaFunction(
        ...     function_type="dropna",
        ...     arguments=DropNaArgs(
        ...         how="any",
        ...         thresh=None,
        ...         subset=["important_col1", "important_col2"]
        ...     )
        ... )
        >>> transformed_df = dropna_fn.transform()(df)

        **Configuration in JSON:**
        ```
        {
            "type": "transform",
            "id": "transform-clean-data",
            "upstream_id": "extract-raw-data",
            "functions": [
                {
                    "function_type": "dropna",
                    "arguments": {
                        "how": "any",
                        "thresh": null,
                        "subset": ["user_id", "email"]
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        type: transform
        id: transform-clean-data
        upstream_id: extract-raw-data
        functions:
          - function_type: dropna
            arguments:
              how: any
              thresh: null
              subset:
                - user_id
                - email
        ```

    Note:
        When 'thresh' is specified, it overrides the 'how' parameter. The thresh
        value specifies the minimum number of non-null values required to keep a row.
        If 'subset' is null, all columns are checked for null values.
    """

    def transform(self) -> Callable:
        """Return a callable function that drops rows with null values from a DataFrame.

        This method creates and returns a transformation function that removes
        rows containing null values based on the configured criteria. The returned
        function can be applied to any DataFrame.

        Returns:
            A callable function that accepts a DataFrame and returns a new DataFrame
            with rows containing null values removed according to the configuration.

        Example:
            >>> dropna_transform = dropna_fn.transform()
            >>> result_df = dropna_transform(input_df)
        """

        def __f(df: DataFrame) -> DataFrame:
            """Apply null value dropping to the DataFrame.

            Args:
                df: Input DataFrame from which to drop rows with null values

            Returns:
                DataFrame with rows containing null values removed based on criteria
            """
            return df.dropna(how=self.arguments.how, thresh=self.arguments.thresh, subset=self.arguments.subset)

        return __f
