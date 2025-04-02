import sqlite3
import time
import threading
import queue


DB_TIMEOUT = 60

class ErrorChecker:
    def __init__(self, setting, logger, rank, llm):
        self.setting = setting
        self.logger = logger
        self.rank = rank
        self.llm = llm

    def execute_sql(self, db_path, predicted_sql, mode, result_queue):
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(predicted_sql)

            if mode == 'is_error':
                result_queue.put("None")

        except sqlite3.Error as e:

            if mode == 'is_error':
                if self.setting.logging:
                    self.logger.PROMPT_FILE.writelines('\n***** ERROR ***** \n' + str(e) + ' \n')
                result_queue.put(str(e))
        finally:
            if conn:
                conn.close()
                
    def is_error(self, predicted_sql, db_id):
        db_path = self.setting.db_path[db_id]
        result_queue = queue.Queue()
        thread = threading.Thread(target=self.execute_sql,
                                args=(db_path, predicted_sql, 'is_error', result_queue))
        thread.daemon = True
        thread.start()
        thread.join(timeout=DB_TIMEOUT)

        if thread.is_alive():
            print(
                "DBQueryTimeoutError: Execution of the SQL query within the 'is_error' function was terminated due to a timeout.")
            # Thread still alive, implying timeout
            return "None"
        else:
            result = result_queue.get()
            return result
        
    def check_execution_error(self, predicted_sql, db_id):
        """
        if error occurs: return True
        if not: return False
        """
        error_result = self.is_error(predicted_sql, db_id)
        if error_result == "None":
            return False
        else:
            return True
