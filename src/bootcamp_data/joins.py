import pandas as pd

def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: str,
    validate: str = "many_to_one",
    suffixes: tuple[str, str] = ("", "_right"),
) -> pd.DataFrame:
    """
    Perform a safe LEFT JOIN with validation.

    Parameters
    ----------
    left : pd.DataFrame
        Left table
    right : pd.DataFrame
        Right table
    on : str
        Join key
    validate : str
        Join validation rule (default: many_to_one)
    suffixes : tuple
        Column suffixes for overlapping names

    Returns
    -------
    pd.DataFrame
        Joined DataFrame
    """
    return left.merge(
        right,
        how="left",
        on=on,
        validate=validate,
        suffixes=suffixes,
    )


