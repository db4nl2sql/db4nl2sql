from openai import OpenAI
import os, warnings
from dotenv import load_dotenv
import re
import json
from util import const
import collections
import json
import re
import sqlite3
from collections import defaultdict
from langchain_ollama import OllamaLLM

DB_TIMEOUT = 60

# V1)
def make_schema_text(db: dict):
    table_names = db['table_names_original']
    column_names = db['column_names_original']

    # {table1: [col1, col2, col3, ...]
    tables = {table_name: [] for table_name in table_names}
    for tbl_idx, col_name in column_names:
        if tbl_idx >= 0:
            tbl_name = table_names[tbl_idx]
            tables[tbl_name].append(col_name)


    table_texts = []
    col_val_expl = []
    foreign_key_texts = []

    for tbl, cols in tables.items():

        col_list = []
        for col in cols:
            col_list.append(col)

        # final column information
        cols_text = ', '.join(col_list)

        # table_text
        append_str = f"# {tbl}({cols_text})"
        if append_str not in table_texts:
            table_texts.append(f"# {tbl}({cols_text})")

        # foreign key information  : based on the tbl
        for i in range(len(db['foreign_keys'])):
            fk_info = db['foreign_keys'][i]
            fk_tbl_idx, fk_col = column_names[fk_info[0]]
            fk_tbl = table_names[fk_tbl_idx]
            ref_tbl_idx, ref_col = column_names[fk_info[1]]
            ref_tbl = table_names[ref_tbl_idx]
            append_str = f"# {fk_tbl}.{fk_col} = {ref_tbl}.{ref_col}"
            if append_str not in foreign_key_texts:
                foreign_key_texts.append(append_str)
        
    schema_text = '# [Schema] \n' + '\n#\n'.join(table_texts)

    if len(foreign_key_texts) > 0:
        schema_text += '\n#\n# [Foreign Keys] \n' + '\n'.join(foreign_key_texts)

    return schema_text

# V2)
def make_schema_text_w_desc(db: dict):
    table_names = db['table_names_original']      
    column_names = db['column_names_original']     

    tables = {t_name: [] for t_name in table_names}
    for col_idx, (tbl_idx, col_name) in enumerate(column_names):
        if tbl_idx >= 0:  
            t_name = table_names[tbl_idx]
            tables[t_name].append((col_name, col_idx))

    for pk_info in db.get('primary_keys', []):
        if isinstance(pk_info, int):
            pk_info = [pk_info]
        for pk_idx in pk_info:
            tbl_idx, pk_col = column_names[pk_idx]
            tbl_name = table_names[tbl_idx]
            if not any(col == pk_col for col, _ in tables[tbl_name]):
                tables[tbl_name].append((pk_col, pk_idx))

    table_texts = []
    for tbl, cols_info in tables.items():
        col_names = [col_name for col_name, _ in cols_info]
        cols_text = ", ".join(col_names)
        table_texts.append(f"# {tbl}({cols_text})")

    foreign_key_texts = []
    for fk_info in db.get('foreign_keys', []):
        fk_tbl_idx, fk_col_name = column_names[fk_info[0]]
        ref_tbl_idx, ref_col_name = column_names[fk_info[1]]
        fk_tbl = table_names[fk_tbl_idx]
        ref_tbl = table_names[ref_tbl_idx]
        fk_text = f"# {fk_tbl}.{fk_col_name} = {ref_tbl}.{ref_col_name}"
        if fk_text not in foreign_key_texts:
            foreign_key_texts.append(fk_text)

    schema_text = "# [Schema]\n" + "\n#\n".join(table_texts)
    if foreign_key_texts:
        schema_text += "\n#\n# [Foreign Keys]\n" + "\n".join(foreign_key_texts)

    return schema_text


# V3 - 1) Renamed View
def make_schema_text_with_view(db: dict, renamed_views: list, cv_flag:bool, cv_result):

    renamed_table_dict = {
        rv["table_name"]: rv for rv in renamed_views
    }

    rename_map = {}
    for rv in renamed_views:
        tbl_name = rv["table_name"]
        for old_name, new_name in rv["column_mapping"]:
            rename_map[(tbl_name, old_name)] = new_name

    schema_lines = []
    for rv in renamed_views:
        schema_lines.append(f"# {rv['schema_text']}")

    # foreign_keys
    foreign_key_lines = []
    for (fk_src, fk_dst) in db['foreign_keys']:
        src_table_idx, src_col = db['column_names_original'][fk_src]
        dst_table_idx, dst_col = db['column_names_original'][fk_dst]

        src_table_name = db['table_names_original'][src_table_idx]
        dst_table_name = db['table_names_original'][dst_table_idx]

        src_col_renamed = rename_map.get((src_table_name, src_col), src_col)
        dst_col_renamed = rename_map.get((dst_table_name, dst_col), dst_col)

        line = f"# {src_table_name}.{src_col_renamed} = {dst_table_name}.{dst_col_renamed}"
        if line not in foreign_key_lines:
            foreign_key_lines.append(line)

    schema_text = "# [Schema]\n" + "\n#\n".join(schema_lines)
    if foreign_key_lines:
        schema_text += "\n#\n# [Foreign Keys]\n" + "\n".join(foreign_key_lines)

    if cv_flag:
        schema_text += '\n#\n#[Customized View DDL]\n'
        schema_text += cv_result['documents'][0][0]

    return schema_text


def make_schema_text_for_rv(db_path: str, db: dict):
    table_names = db['table_names_original']
    column_names = db['column_names_original']
    tables = {t: [] for t in table_names}

    for i, (tbl_i, col) in enumerate(column_names):
        if tbl_i >= 0:
            tables[table_names[tbl_i]].append((col, i))

    for pk in db.get('primary_keys', []):
        if isinstance(pk, int):
            pk = [pk]
        for col_idx in pk:
            tbl_i, pk_name = column_names[col_idx]
            tbl_name = table_names[tbl_i]
            if not any(c == pk_name for c, _ in tables[tbl_name]):
                tables[tbl_name].append((pk_name, col_idx))

    # [Schema]
    lines = ["# [Schema]"]
    for t, cols in tables.items():
        lines.append(f"# {t}({', '.join(c for c, _ in cols)})")

    # [Foreign Keys]
    fk = db.get('foreign_keys', [])
    if fk:
        lines.append("#\n# [Foreign Keys]")
        fk_set = set()
        for f in fk:
            fk_tbl_i, fk_col = column_names[f[0]]
            ref_tbl_i, ref_col = column_names[f[1]]
            fk_set.add(f"# {table_names[fk_tbl_i]}.{fk_col} = {table_names[ref_tbl_i]}.{ref_col}")
        lines.extend(fk_set)

    # [Column Value Examples]
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    seen = set()
    examples = []

    for t, cols in tables.items():
        cur.execute(f"PRAGMA table_info('{t}')")
        info = {r[1]: (r[2].lower() if r[2] else '') for r in cur.fetchall()}
        for c, _ in cols:
            if c not in seen and info.get(c) not in ["int","integer","float","real","double","numeric"]:
                cur.execute(f'SELECT DISTINCT "{c}" FROM "{t}" WHERE "{c}" IS NOT NULL LIMIT 5')
                vals = [str(r[0]) for r in cur.fetchall() if r[0] and len(str(r[0])) < 50]
                if vals:
                    examples.append(f"# {c}: {', '.join(vals)}")
                seen.add(c)

    conn.close()

    if examples:
        lines.append("#\n# [Column Value Examples]")
        lines.extend(examples)

    return "\n".join(lines)

    
def llm_call_max4096(llm, model: str, prompt: str):
    if model == "llama3.1:8b" or model == "llama3.1:70b" or model == "llama3.1:405b": # 
        response = llm.invoke(prompt)

    else:
        response = llm.chat.completions.create(
        model=model,
        messages=[
            {"role": "user", "content": prompt},
        ],
        max_tokens=4096,
        temperature=0
    )
        
    return response

def llm_call(llm, model: str, prompt: str):
    if model == "llama3.1:8b" or model == "llama3.1:70b" or model == "llama3.1:405b": # 
        response = llm.invoke(prompt)

    else:
        response = llm.chat.completions.create(
            model=model,
            messages=[
                {"role": "user", "content": prompt},
            ],
            temperature=0
        )

    return response



def connect_openaiAPI(rank):
    """
    Connect OpenAI API
    """
    load_dotenv()
    OPENAI_API_KEY = os.getenv(f"OPENAI_API_KEY_{rank}")
    warnings.filterwarnings("ignore")

    return OpenAI(api_key=OPENAI_API_KEY)

def connect_ollama(setting):
    llm = OllamaLLM(
        model=setting.model,
        temperature=0,
    )

    return llm

def load_data_set(setting, rank):
    # db_path, schema: global variable
    # data :            local variable (different per rank)

    json_dev_data = {}
    # load question and db id
    with open(setting.base_dir + 'data/dev.json', 'r') as file:  
        json_dev_data = json.load(file)

    # split the dataset for parallel execution
    n = len(json_dev_data)
    n_per_process = n // setting.num_cpus
    start_idx = rank * n_per_process

    end_idx = n if (rank == setting.num_cpus - 1) else (start_idx + n_per_process)

    data = [(data['question_id'], data['question'], data['db_id'], data['evidence'])
            for data in json_dev_data[start_idx:end_idx]]

    return data

def convert_to_base_query_using_llm(qsql:str, db_id: str, setting, logger, llm, view_name):
    view = const.get_customized_view(view_name)
    prompt = const.customized_view_to_base_prompt.format(view_schema=view, view_query=qsql)
    response = llm_call_max4096(llm, setting.model, prompt)

    if setting.logging and logger.PROMPT_FILE is not None:
        logger.PROMPT_FILE.writelines("\n***** FINAL PROMPT ***** \n" + prompt + '\n')
        logger.PROMPT_FILE.writelines("\n***** RESPONSE ***** \n" + str(response))
        logger.PROMPT_FILE.writelines("\n\n***** TOKEN INFO ***** \n" + str(response.usage) + '\n')
        logger.calculate_token(response)

    # Extract the content (expected to be an SQL query in string)
    if setting.model == "llama3.1:8b" or setting.model == "llama3.1:70b" or setting.model == "llama3.1:405b": # 
        qsql = response

    else: #gpt 
        qsql = response.choices[0].message.content.replace('\n', ' ')

    sql_pattern = r"```sql\n(.*?)\n```" 
    match = re.search(sql_pattern, qsql, re.DOTALL)
    if match: 
        qsql = match.group(1).strip() 
        
    qsql = qsql.replace("AdmFName1 || ' ' || AdmLName1 AS AdminFullName", "AdmFName1, AdmLName1")

    if qsql.startswith("```sql"):
        qsql = qsql.split("```sql")[1].split("```")[0].strip()
    # if qsql doesn't start with 'SELECT', manually add 'SELECT' clause.
    if not qsql.upper().startswith("SELECT"):
        qsql = 'SELECT ' + qsql

    return qsql

def convert_to_base_query_manually(qsql:str, db_id:str, setting, mode:str):
    if mode == "per_table_manual_rename":
        qsql = qsql.replace("frpm_v", "frpm")
        qsql = qsql.replace("schools_v", "schools")
        qsql = qsql.replace("satscores_v", "satscores")


        origin_cols = [item[1] for item in setting.view_col_info[db_id]]
        renamed_cols = [item[2] for item in setting.view_col_info[db_id]]
        # print(qsql)
        for i, renamed_col in enumerate(renamed_cols):
            if renamed_col in qsql:
                if origin_cols[i].startswith("`"):
                    qsql = qsql.replace(renamed_col, f"{origin_cols[i]}")
                else:
                    qsql = qsql.replace(renamed_col, f"{origin_cols[i]}")
        qsql = qsql.replace("``", "`")

    return qsql


def parse_json_res(response_text: str):
    """
    Extracts a JSON list from a response containing markdown-style code blocks.
    Tries to match ```<lang>\n[json]``` or fallback to raw parsing.
    """
    # Match any code block regardless of language label (e.g., json, python)
    code_block_match = re.search(r"```(?:\w+)?\s*(.*?)```", response_text, re.DOTALL)

    if code_block_match:
        code_block_content = code_block_match.group(1).strip()

        try:
            data = json.loads(code_block_content)
            if isinstance(data, list):
                return data
            else:
                print("Parsed JSON is not a list.")
                return []
        except json.JSONDecodeError as e:
            print(f"JSON decoding failed: {e}")
            print("Raw code block content:", code_block_content)
            return []
    else:
        # Fallback: try parsing the entire response_text
        try:
            data = json.loads(response_text)
            if isinstance(data, list):
                return data
            else:
                print("Parsed content is not a list.")
                return []
        except json.JSONDecodeError as e:
            print(f"JSON decoding failed: {e}")
            print("Raw response:")
            print(response_text)
            return []


