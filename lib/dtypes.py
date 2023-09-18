### Useful datatype aliases

CensusTableName = str
"""
A variable name is a string which is also a table name
in the CR database.

Example: 'b01001'
"""


CensusVariableName = str
"""
A variable name is a string which is also a column name
in a census-style table.

Example: 'b01001001' - This is from the table 'b01001'
"""


Indentation = int
"""
This is the value that a variable is "indented" as it would
show up on the ACS tables that you'd find online.

Though by the name you'd think that this merely about how a value
would show up printed on a table, it actually represents the node 
depth in the tree-like structure of the census data.

Example: For table 'b01001', variable 'b01001002,' 'Male', has 
indentation of 1 because its parent variable 'b01001001' is 'Total'
and top of the tree. Variable 'b01001003,' 'Under 5 years' has 
indentation level 2, and it is 'indented' under the 'Male' variable, 
which is its parent column.

This maybe isn't the best way to represent this structure, but it is 
used throughout Census Reporter.
"""
