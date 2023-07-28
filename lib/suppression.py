import pandas as pd

from .dtypes import Indentation, CensusVariableName


class Pivot:
    """
    'Pivots' are the column with the highest value below the
    suppression threshold. However, sometimes all the values are above
    the suppression threshold and other times, below. This datatype 
    handles all three situations.
    """


class AllAbove(Pivot):
    pass


class AllBelow(Pivot):
    """
    Technically this case would be handled by the 'RightOn' workflow
    using the 0-indented column as the base, but it makes it clearer.
    """


class RightOn(Pivot):
    __match_args__ = ("pivot",)

    def __init__(self, pivot: str):
        self.pivot = pivot


def find_pivot_column(row: pd.Series, threshold=6) -> Pivot:
    """
    Look across the row and find the highest value below the threshold.
    Return the variable name of that value.
    """

    # filter out columns by label you don't want to apply suppression to.
    value_columns = row.loc[
        [
            label
            for label in row.index
            if not ((label == "geoid") | (label == "index"))
        ]
    ]
    # Find value columns below threshold
    below_threshold = value_columns[value_columns.astype(int) < threshold]

    if len(below_threshold) == 0:
        # If there are no values below the threshold, return AllAbove()
        return AllAbove()

    if len(below_threshold) == len(value_columns):
        # If all values are below the threshold, return AllBelow()
        return AllBelow()

    # Filter to all values below the threshold
    # Return the index of the highest of these values so everything below will be suppressed
    # Since the index of this series is a list of variable names, the index will be as well.

    return RightOn(below_threshold.astype(int).idxmax())


def find_mute_indent(
    pivot_column: CensusVariableName, column_metadata: pd.DataFrame
) -> Indentation:
    """
    Given the variable name, look up in the column_metadata table what
    indent values of which need to be suppressed.
    """
    try:
        return column_metadata[column_metadata["variable_name"] == pivot_column][
            "indentation"
        ].values[0]
    except IndexError:
        raise IndexError(f"{pivot_column} not found in column_metadata")


def find_mute_columns(
    column_metadata: pd.DataFrame, mute_indent: Indentation
) -> list[CensusVariableName]:
    return column_metadata[(column_metadata["indentation"] >= mute_indent)]["variable_name"]


def mute_small_values(
    row: pd.Series, column_metadata: pd.DataFrame, threshold: int = 6
) -> pd.Series:
    """
    Applies the mute process to a row.
    """

    pivot = find_pivot_column(row, threshold=threshold)

    # Copy the row before making changes.
    safe = row.copy()

    match pivot:
        case AllAbove():
            return safe
        
        case AllBelow():
            # If no pivot is found, i.e all values are
            # below threshold, replace_all_values except geoid with None
            safe.loc[[position for position in safe.index if position != 'geoid']] = None

            return safe

        case RightOn(pivot_column):
            mute_indent = find_mute_indent(pivot_column, column_metadata)

    to_mute = find_mute_columns(column_metadata, mute_indent)
    safe.loc[to_mute.str.lower()] = None

    return safe


def apply_suppression(
    df: pd.DataFrame, column_metadata: pd.DataFrame, threshold: int = 6
) -> pd.DataFrame:
    return pd.DataFrame(
        [mute_small_values(row, column_metadata, threshold) for _, row in df.iterrows()]
    )
