from transformslib.transforms.base import TableTransform

class CustomTransform(TableTransform):
    """
    CustomTransform
    ---------------
    Description: Briefly describe what this transform does.

    Arguments:
        - arg1 (type): Description of argument 1.
        - arg2 (type): Description of argument 2.
        # Add more arguments as needed.

    Testable: [True/False]
        - If True, add a test(self, supply_frames, **kwargs) method.
        - If False, remove the test method.

    Execution Test: [True/False]
        - If True, ensure arguments are passed correctly.
        - If unsure, leave as True and contact the framework maintainer.
    """

    def __init__(self, arg1, arg2):
        """
        Initialise a CustomTransform.

        Args:
            arg1 (type): Description of argument 1.
            arg2 (type): Description of argument 2.
            # Add more arguments as needed.
        """
        super().__init__(
            "CustomTransform1",
            "Brief description of what this transform does.",
            [arg1, arg2],  # TODO: List all variables/columns used by this transform
            "CstmTrfm1", #custom code for this transform type
            testable_transform=True  # TODO: Set to True if you will implement a test method
        )
        # Save arguments as instance variables
        self.arg1 = arg1
        self.arg2 = arg2
        # TODO: Add more arguments as needed

    def error_check(self, supply_frames, **kwargs):
        """
        Validate that required columns/conditions exist in the DataFrame.

        Args:
            supply_frames: TableCollection containing all tables.
            kwargs: Should include 'df' (table name).

        Returns:
            True if checks pass, otherwise raises ValueError.
        """
        table_name = kwargs.get('df')
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")
        # TODO: Add custom error checks below
        # Example: Check if arg1 is a column in the DataFrame
        # if self.arg1 not in supply_frames[table_name].columns:
        #     raise ValueError(f"Column '{self.arg1}' not found in DataFrame '{table_name}'")
        return True

    def transforms(self, supply_frames, **kwargs):
        """
        Apply the transformation logic to the DataFrame.

        Args:
            supply_frames: TableCollection containing all tables.
            kwargs: Should include 'df' (table name).

        Returns:
            Updated TableCollection.
        """
        table_name = kwargs.get('df')
        self.target_tables = [table_name]
        input_df = supply_frames[table_name]
        # TODO: Implement transformation logic here
        # Example: output_df = input_df.drop(columns=[self.arg1])
        output_df = input_df  # Replace with actual logic

        supply_frames[table_name] = output_df
        supply_frames[table_name].add_event(self)
        return supply_frames

    # Uncomment if testable_transform=True
    # def test(self, supply_frames, **kwargs) -> bool:
    #     """
    #     Test that the transformation was applied correctly.
    #     Args:
    #         supply_frames: TableCollection containing all tables.
    #         kwargs: Should include 'df' (table name).
    #     Returns:
    #         True if test passes, False otherwise.
    #     """
    #     table_name = kwargs.get('df')
    #     if not table_name:
    #         return False
    #     # TODO: Implement actual test logic
    #     # Example: return self.arg1 not in supply_frames[table_name].columns
    #     return True
