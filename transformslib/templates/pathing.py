import os

def apply_formats(input_str:str) -> str:
    """
    Apply the pathing formats to the input string.
    """
    try:
        formatted_str = input_str.replace("{prodtest}", os.environ.get("TNSFRMS_PROD", "prod"))
        formatted_str = formatted_str.replace("{job_id}", str(os.environ.get("TNSFRMS_JOB_ID", 1)))
        formatted_str = formatted_str.replace("{run_id}", str(os.environ.get("TNSFRMS_RUN_ID", 1)))
        return formatted_str
    except KeyError as e:
        raise KeyError(f"PT001 Key Error. Environment variable update for pathing is not working: Potentially missing key {e}")
    except Exception as e:
        raise Exception(f"PT002 General Error in applying path formats: {e}")
