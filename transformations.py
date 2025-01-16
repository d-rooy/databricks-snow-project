# transformations.py

from pyspark.sql.functions import expr

def extract_field(value_col, marker, offset, length):
    """
    Extracts a substring from the value column based on a marker, offset, and length.
    """
    # The usage of `expr` and `instr` is directly carried from your original script
    return expr(f"substring({value_col}, instr({value_col}, '{marker}') + {offset}, {length})")
