import json
import os, warnings
from dotenv import load_dotenv
import chromadb
import chromadb.utils.embedding_functions as embedding_functions
import pandas as pd
import collections
from util import const, util


# PreProsessor
class Setting():
    def __init__(self, mode, logging, num_cpus, model, result_file):

        # args
        load_dotenv()
        self.mode = mode
        self.logging = logging
        self.num_cpus = num_cpus
        self.model = model
        self.result_file = result_file

        # file, directory setting
        self.result_file = result_file
        self.base_dir = os.getenv("BASE_DIR")
        self.result_path = self.base_dir + 'exp_result/'
        self.final_log_file = None
        self.db_path = {}
        self.renamed_view_ddl = ""

        # schema
        self.schema = {}
        self.renamed_view = {}
        self.unified_view = {}

        # Initialization Functions
        self.load_schema_info()

        if self.mode == "view":
            self.create_renamed_view()
            self.create_unified_view()
        


    def load_col_desc_from_csv(self,  db_id):
        desc_dir = self.base_dir + '/data/dev_databases/' + db_id + '/database_description'
        for _, _, files in os.walk(desc_dir):
            self.schema[db_id]['db_desc'] = {}
            for file in files:
                col_desc = pd.read_csv(desc_dir + '/' + file, encoding='utf-8', encoding_errors='ignore')
                table_name = file[:-4]
                self.schema[db_id]['db_desc'][table_name] = {}
                if table_name == '.DS_S':
                    continue
                table_idx = self.schema[db_id]["table_names_original"].index(table_name)

                for col_idx, col in col_desc.iterrows():

                    col_name = col['original_column_name'].strip()
                    self.schema[db_id]['db_desc'][table_name][col_name] = {
                        'original_column_name': col['original_column_name'].strip()
                    }
                    if 'column_description' in col_desc.columns:
                        self.schema[db_id]['db_desc'][table_name][col_name]['column_description'] = col[
                            'column_description']
                    if 'data_format' in col_desc.columns:
                        self.schema[db_id]['db_desc'][table_name][col_name]['data_format'] = col['data_format']
                    if 'value_description' in col_desc.columns:
                        self.schema[db_id]['db_desc'][table_name][col_name]['value_description'] = col[
                            'value_description']

    def load_col_desc_from_json(self):
        json_desc_file = self.base_dir + 'exp_result/column_meaning.json'

        # import json data from column_meaning.json file
        with open(json_desc_file, 'r') as f:
            col_desc_json = json.load(f)

        for k, v in col_desc_json.items():
            db_id, table_name, col_name = k.split('|')
            if table_name in self.schema[db_id]['db_desc'].keys() and col_name in self.schema[db_id]['db_desc'][
                table_name].keys():
                self.schema[db_id]['db_desc'][table_name][col_name]['column_description'] = v 

    def load_schema_info(self):
        """
        load_col_desc_from_csv(): load column info from csv file, column description from csv file can be incorrect
        insert_col_vals_to_es(): insert col-vals to prevent value error
        load_col_desc_from_json(): column description from json file, specific column description
        create_col_desc_collection(): create vectordb storage collection for similarity search
        """

        # load db path to open
        with open(self.base_dir + 'data/dev_tables.json', 'r') as file:
            json_table_data = json.load(file)

        for db_data in json_table_data:
            db_id = db_data['db_id']
            # sqlite path
            if db_id not in self.db_path.keys():
                self.db_path[db_id] = os.path.join(self.base_dir, 'data/dev_databases',
                                                   db_data['db_id'], db_data['db_id'] + '.sqlite')
            # schema info
            if db_id not in self.schema.keys():
                self.schema[db_id] = db_data
                self.load_col_desc_from_csv(db_id)

        self.load_col_desc_from_json()


    def create_renamed_view(self):
        for db_id, _ in self.schema.items():
            # 
            db = self.schema[db_id]
            schema_info = util.make_schema_text_for_rv(self.db_path[db_id], db)
            
            # call llm
            prompt = const.renamed_view_creation_prompt.format(schema_info=schema_info)
            llm = util.connect_openaiAPI(0)
            res = util.llm_call(llm, self.model, prompt)
            parsed_res = util.parse_json_res(res.choices[0].message.content)

            prompt2 = const.renamed_view_disambiguation_prompt.format(renamed_view_data=parsed_res)
            res2 = util.llm_call(llm, self.model, prompt2)
            final_res = util.parse_json_res(res2.choices[0].message.content)

            # 
            self.renamed_view[db_id] = final_res
            print(f"Renamed View Creation {db_id} done")
    
    def create_unified_view_per_db(self, db):
        table_names = db['table_names_original']
        column_specs = db['column_names_original']  # (table_idx, col_name) 
        foreign_keys = db.get('foreign_keys', [])

        valid_cols = [(tid, cname) for (tid, cname) in column_specs if tid >= 0]
        used_colnames = set()
        colname_map = {}
        for (tid, cname) in valid_cols:
            if cname not in used_colnames:
                colname_map[(tid, cname)] = cname
                used_colnames.add(cname)
            else:
                new_name = f"{cname}_{table_names[tid]}"
                suffix_idx = 1
                while new_name in used_colnames:
                    new_name = f"{cname}_{table_names[tid]}_{suffix_idx}"
                    suffix_idx += 1
                colname_map[(tid, cname)] = new_name
                used_colnames.add(new_name)

        adjacency = {i: {} for i in range(len(table_names))}
        for (c1, c2) in foreign_keys:
            t1, col1 = column_specs[c1]
            t2, col2 = column_specs[c2]
            if t1 < 0 or t2 < 0:
                continue
            adjacency[t1].setdefault(t2, []).append((col1, col2))
            adjacency[t2].setdefault(t1, []).append((col2, col1))

        visited = [False] * len(table_names)
        join_sequence = []  # (table_idx, parent_idx, conds)

        for start_table in range(len(table_names)):
            if not visited[start_table]:
                visited[start_table] = True
                queue = collections.deque([start_table])
                join_sequence.append((start_table, None, []))

                while queue:
                    cur = queue.popleft()
                    for neigh, conds in adjacency[cur].items():
                        if not visited[neigh]:
                            visited[neigh] = True
                            join_sequence.append((neigh, cur, conds))
                            queue.append(neigh)

        alias_map = {i: f"t{i}" for i in range(len(table_names))}
        select_clauses = []
        for (tid, cname) in valid_cols:
            alias = alias_map[tid]
            new_cname = colname_map[(tid, cname)]
            select_clauses.append(f"{alias}.{cname} AS {new_cname}")

        from_clauses = []
        join_steps = []

        if not join_sequence:
            unified_ddl = "CREATE VIEW unified_view AS SELECT 1 WHERE 0"
        else:
            first_table, first_parent, first_conds = join_sequence[0]
            from_clauses.append(f"FROM {table_names[first_table]} {alias_map[first_table]}")
            join_steps.append({
                'table_name': table_names[first_table],
                'alias': alias_map[first_table],
                'parent_table': None,
                'join_type': 'FROM',
                'join_conditions': []
            })

            for idx in range(1, len(join_sequence)):
                table_idx, parent_idx, conds = join_sequence[idx]
                tname = table_names[table_idx]
                alias = alias_map[table_idx]

                if parent_idx is None:
                    from_clauses.append(f"INNER JOIN {tname} {alias} ON 1=1")
                    join_steps.append({
                        'table_name': tname,
                        'alias': alias,
                        'parent_table': None,
                        'join_type': 'INNER JOIN',
                        'join_conditions': []
                    })
                else:
                    parent_alias = alias_map[parent_idx]
                    parent_tname = table_names[parent_idx]
                    if conds:
                        on_conditions = []
                        for (c1, c2) in conds:
                            on_conditions.append(f"{parent_alias}.{c1} = {alias}.{c2}")
                        on_str = " AND ".join(on_conditions)
                        from_clauses.append(f"INNER JOIN {tname} {alias} ON {on_str}")
                        join_steps.append({
                            'table_name': tname,
                            'alias': alias,
                            'parent_table': parent_tname,
                            'join_type': 'INNER JOIN',
                            'join_conditions': [
                                {
                                    'left_table': parent_tname,
                                    'left_col': c1,
                                    'right_table': tname,
                                    'right_col': c2
                                }
                                for (c1, c2) in conds
                            ]
                        })
                    

            unified_ddl = (
                "CREATE VIEW unified_view AS\n"
                "SELECT\n  " + ",\n  ".join(select_clauses) + "\n" +
                " " + "\n ".join(from_clauses)
            )

        schema_lines = [colname_map[(tid, cname)] for (tid, cname) in valid_cols]
        schema_text = "[Columns]\n" + "\n".join(schema_lines)

        column_mapping = []
        for (tid, cname) in valid_cols:
            column_mapping.append([
                table_names[tid],
                cname,
                colname_map[(tid, cname)]
            ])
        fkeys_original = []
        for (c1, c2) in foreign_keys:
            t1, col1 = column_specs[c1]
            t2, col2 = column_specs[c2]
            if t1 < 0 or t2 < 0:
                continue
            fkeys_original.append((table_names[t1], col1, table_names[t2], col2))

        return {
            'unified_view_ddl': unified_ddl,
            'schema_text': schema_text,
            'column_mapping': column_mapping,
            'join_steps': join_steps,
            'foreign_keys_original': fkeys_original
        }

    def create_unified_view(self):
        for db_id, db_info in self.schema.items():
            schema_info = self.create_unified_view_per_db(db_info)
            self.unified_view[db_id] = schema_info
        
    
    def merge_result_file(self):
        result_json = {}
        for rank in range(self.num_cpus):
            with open(self.result_path + f"result_file_{rank}.json", 'r') as file:
                result_json.update(json.load(file))

        with open(self.result_path + self.result_file, 'w') as file:
            json.dump(result_json, file)

   
    def merge_log_file(self):
        prompts = ""
        for rank in range(self.num_cpus):
            with open(f'src/log/prompt_log_{rank}.txt', 'r') as prompt_file:
                prompts += prompt_file.read() + '\n'

        with open('src/log/prompt_log_' + self.result_file[:-5] + '.txt', 'w') as result_file:
            result_file.write(prompts)

        prompt_file.close()
