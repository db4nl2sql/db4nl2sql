import re


general_tip = '''## Remember the following caution and do not make same mistakes:
# 1. If there are blank space between column name do not concatenate the words. Instead, use backtick(`) around the column name.
# 2. Make sure that the column belongs to the corresponding table in given [Schema].
# 3. When doing division, type cast numerical values into REAL type using 'CAST'.
# 4. Avoid choosing columns that are not specifically requested in the question. 
# 5. Return sqlite SQL query only without any explanation.
# 6. Try to use DISTINCT if possible.
# 7. Please make sure to verify that the table-column matches in the generated SQL statement are correct. Every column used must exist in the corresponding table.
# 8. If there is no example of a column value, it likely indicates that the column is of a numeric type—please make sure to take this into account. 
     For example, even if a column name ends with something like 'YN', if there is no example provided in the column example, it means the values are represented as 0 and 1—so be sure to treat it as numeric (Boolean) accordingly.
'''

general_tip_uv = '''## Remember the following caution and do not make same mistakes:
# 1. If there are blank spaces in the column name, do not concatenate the words. Instead, use backtick(`) around the column name.
# 2. Make sure that the column belongs to the single table (unified_view) in given [Columns].
# 3. When doing division, type cast numerical values into REAL type using 'CAST'.
# 4. Avoid choosing columns that are not specifically requested in the question.
# 5. Return sqlite SQL query only without any explanation.
# 6. Try to use DISTINCT if possible.
'''

renamed_view_creation_prompt = '''
You are given a table schema containing column names and their descriptions.
Your task is to generate a Renamed View for each table individually, renaming ambiguous or unclear column names to make them more intuitive and self-explanatory.

Please follow these guidelines:

1. For each table, create a view that includes all columns, but only rename the ones that are **highly ambiguous** or **unclear**.
2. The new column names should be:
   - Concise (within 20 to 50 characters),
   - Descriptive enough to convey the column's meaning,
   - And include format/type indicators (e.g., _YYYY_MM_DD ).
3. Even if a column is not renamed, you must include it in the mapping for completeness.
4. For each table, return a JSON object with the following fields:
   - "table_name": the original table name,
   - "renamed_view_ddl": the SQL DDL statement to create the renamed view,
   - "schema_text": a simplified textual representation of the view schema in the format: table_name(renamed_column1, renamed_column2, ...),
   - "column_mapping": a list of [original_column_name, final_column_name] pairs for **all** columns in the table.
5. Avoid duplicate columns in the resulting view. If semantically similar columns appear in different tables, embed the table name into the column name to avoid duplication.


The "column_mapping" and "schema_text" should include every column in the view, whether it was renamed or not. 
Only rename columns when necessary, and leave the rest unchanged.

[Sample]

Sample Input:
You are given the following schema:

employee.id: unique identifier for the employee.
employee.name: full name of the employee.
employee.doj: date the employee joined the company.
employee.dept: department the employee belongs to.
employee.salary: monthly salary of the employee.
department.id: unique identifier for the department.
department.name: name of the department.
department.location: city where the department is located.

Sample Output:
[
  {{
    "table_name": "employee",
    "renamed_view_ddl": "CREATE VIEW employee_renamed_view AS SELECT id AS id_employee, name AS name_employee, doj AS date_joined_company, dept AS department, salary FROM employee;",
    "schema_text": "employee(id, name, date_joined_company, dept, salary)",
    "column_mapping": [
      ["id", "id_employee"],
      ["name", "name_employee"],
      ["doj", "date_joined_company"],
      ["dept", "department"],
      ["salary", "salary"]
    ]
  }},
  {{
    "table_name": "department",
    "renamed_view_ddl": "CREATE VIEW department_renamed_view AS SELECT id AS id_department, name AS name_derpartment, location FROM department;",
    "schema_text": "department(id, name, location)",
    "column_mapping": [
      ["id", "id_department"],
      ["name", "name_derpartment"],
      ["location", "location"]
    ]
  }}
]

Now, based on the instructions above, generate renamed view for the following schema and return the result in the specified **JSON format**:

{schema_info}
'''

renamed_view_disambiguation_prompt = """
You are given a list of tables, each with:
- "table_name"
- "renamed_view_ddl": a CREATE VIEW SQL string with aliases for renamed columns,
- "schema_text": a summary of the renamed columns,
- "column_mapping": a list of [original_col, renamed_col] pairs.

Your task is to disambiguate any column names that are **duplicated across different tables**.

### Disambiguation Rule:
If two or more tables have the same column name on the **left-hand side** of column_mapping (i.e., original column),  
append the table name as a suffix using the format `{{column}}_{{table_name}}` in the **right-hand-side** of column_mapping.  
If the column name already ends with the table name, leave it unchanged.

### Instructions:
1. Apply the renaming only to the **right-hand side (renamed_col)** of column_mapping.
2. Update both "renamed_view_ddl" and "schema_text" to reflect the new disambiguated column names.
3. Maintain the same structure and ordering: return a Python list of dicts in **exactly the same format**.
4. Do not modify table names or the left-hand side (original column names).
5. Use only double quotes for the JSON object output.
6. Especially for the first one or two columns, which are likely to be primary keys or foreign keys, make sure to strictly follow the {{column}}_{{table_name}} format.

Now fix the following input and return the result in the specified **JSON format**:
{renamed_view_data}
"""



evidence_prompt = '''
# [External Knowledge]
# Keep in mind the following external knowledge and do not forget. Generate SQL using the external knowledge : 
# '''

final_prompt = """### Complete sqlite SQL query only and with no explanation.
{general_tip}
{schema_info}{few_shot_prompt}

### Question: {qnl}
### Knowledge Evidence: {evidence}
### SQL:''' 
"""

view_to_base_propmt = """
The following query is based on renamed_col and represents a query against the view.
We need to convert view into the original table and column information.
Make sure the logic is correct—no mistakes are allowed.

Additionally, when converting from the view to the original table format, refer to the PK-FK information provided below.  
When creating the revised query, do not join all three tables by default; **only** include the necessary joins (or constraints) for the tables whose columns are actually used.  

For example, if there are PK-FK constraints like:
- a.aa = b.bb  
- b.bb = c.cc  
but the query only uses columns from tables a and b, then only a.aa = b.bb should be included in the final query.

It has to be correct—no mistakes allowed.
- Return only the final SQL query, with no additional explanation or text!

# mapping info
{mapping_info}


--------------------------------------------------
View Query: {view_query}
Revised SQL:


"""


