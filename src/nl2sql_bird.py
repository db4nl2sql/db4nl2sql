import json
from tqdm import tqdm
import argparse, sys
import multiprocessing as mp
import time

from util import const, logger, preprocessor, util
from agent import SchemaLinker, SQLGenerator, ErrorChecker


class NL2SQL:

    def __init__(self, setting, rank: int = 0, logger=None) -> None:
        self.setting = setting  # global setting
        self.logger = logger
        self.rank = rank  # rank = 0 : default
        self.PROMPT_FILE = None  # log file

        self.llm = None  # llm (different per rank)
        self.data = {}  # different per rank

        self.schema_linker = None
        self.error_corrector = None
        self.few_shot_rag_controller = None
 
    def execute(self, result_file_name: str) -> None:
        """
        [params]
        - model: 'gpt-4o'
        - result_file_name: name of the result txt file. SQL Queries
        """
        result_dict = {}  # { "question_id" : "pred_sql" }

        self.schema_linker = SchemaLinker.SchemaLinker(self.setting, self.logger, self.rank, self.llm)
        self.sql_generator = SQLGenerator.SQLGenerator(self.setting, self.logger, self.rank, self.llm)
        self.error_checker = ErrorChecker.ErrorChecker(self.setting, self.logger, self.rank, self.llm)
        

        # baseline, baseline-w-desc
        # Evaluation 
        for question_id, qnl, db_id, evidence in tqdm(self.data, total=len(self.data), desc="Processing..."):

            # V1) baseline V2) baseline-w-desc
            if self.setting.mode == "baseline" or self.setting.mode == "baseline-w-desc":
                schema_info = self.schema_linker.get_refined_schema_info(db_id, mode = self.setting.mode)
                predictedSQL = self.sql_generator.generate_SQL(qnl, schema_info, evidence)
            
            # V3) View
            elif self.setting.mode == "view": 
                schema_info, v_type = self.schema_linker.get_view_info(db_id, qnl)
                predictedSQL = self.sql_generator.generate_SQL_for_view(db_id, self.setting, self.logger, self.llm, qnl, schema_info, evidence, v_type)
                predictedSQL = self.sql_generator.decompose_to_base_table(predictedSQL, db_id, v_type)

                error_occurred = self.error_checker.check_execution_error(predictedSQL, db_id)
                if error_occurred:
                    schema_info, v_type = self.schema_linker.get_unified_view_info(db_id)
                    predictedSQL = self.sql_generator.generate_SQL_for_view(db_id, self.setting, self.logger, self.llm, qnl, schema_info, evidence, v_type)
                    predictedSQL = self.sql_generator.decompose_to_base_table(predictedSQL, db_id, v_type)

            # # result
            result_dict[str(question_id)] = predictedSQL + '\t----- bird -----\t' + db_id # FIXME

            # logging
            if self.setting.logging:
                self.logger.print_final_prompt(qnl, predictedSQL)

        # write result in result file
        with open(self.setting.result_path + result_file_name, 'w') as file:
            json.dump(result_dict, file)

        return None


def worker(setting, result_file_name, rank):
    print(f"process-{rank}-started")

    # model object creation / setting
    task = NL2SQL(setting, rank)
    if setting.model == "llama3.1:8b" or setting.model == "llama3.1:70b" or setting.model == "llama3.1:405b": 
        task.llm = util.connect_ollama(setting)
    else: #gpt
        task.llm = util.connect_openaiAPI(rank)  
    task.data = util.load_data_set(setting, rank)
    task.logger = logger.Logger(setting, rank)

    # execute NL2SQL task
    task.execute(result_file_name)


if __name__ == "__main__":
    # args
    parser = argparse.ArgumentParser()
    parser.add_argument('--logging', help='', default=0)
    parser.add_argument('--model', help='')
    parser.add_argument('--schema_mode', help='', default='baseline')
    parser.add_argument('--num_cpus', help='', default=1)
    parser.add_argument('--result_file', help='', default='pred.json')
    args = parser.parse_args()


    setting = preprocessor.Setting(args.schema_mode, bool(int(args.logging)), int(args.num_cpus), args.model, args.result_file)

    processes = []

    # experiment
    start = time.time()
    for rank in range(setting.num_cpus):
        partial_result_file = f'result_file_{rank}.json'
        print("process starts...")
        p = mp.Process(target=worker, args=(setting, partial_result_file, rank))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()
    end = time.time()
    print("execution time: %f s" % (end - start))

    # Merge the results from each processes
    setting.merge_result_file()
    if setting.logging:
        setting.merge_log_file()