class Logger():
    def __init__(self, setting, rank=0) -> None:
        self.num_cpus = setting.num_cpus
        self.logging = setting.logging
        self.rank = rank

        self.PROMPT_FILE = None
        self.total_tokens = 0
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self.single_prompt_tokens = 0
        self.single_completion_tokens = 0
        self.cnt = 0

        self.create_log_file()  
    def create_log_file(self):
        if not self.logging:
            return

        file_name = f'src/log/prompt_log_{self.rank}.txt'
        self.PROMPT_FILE = open(file_name, 'w')

    def calculate_token(self, response):
        self.total_tokens += response.usage.total_tokens
        self.total_prompt_tokens += response.usage.prompt_tokens
        self.total_completion_tokens += response.usage.completion_tokens
        self.single_prompt_tokens += response.usage.prompt_tokens
        self.single_completion_tokens += response.usage.completion_tokens
        self.cnt += 1

    def print_final_prompt(self, qnl, predictedSQL):
        self.PROMPT_FILE.write("\n***** FINAL SQL QUERY *****\n")
        self.PROMPT_FILE.write(qnl + '\n' + predictedSQL + '\n\n')
        self.print_token_monitor()

    def print_token_monitor(self):
        self.PROMPT_FILE.write("***** TOKEN MONITORING *****\n")
        self.PROMPT_FILE.write(
            f"TOKEN PER ITERATION    :  {self.single_prompt_tokens}, {self.single_completion_tokens}\n")
        self.PROMPT_FILE.write(
            f"TOTAL                  :  {self.total_tokens}, {self.total_prompt_tokens}, {self.total_completion_tokens}\n")
        self.PROMPT_FILE.write(
            f"AVG                    :  {self.total_tokens / self.cnt}, {self.total_prompt_tokens / self.cnt}, {self.total_completion_tokens / self.cnt}\n")
        self.PROMPT_FILE.write(
            "*********************************************************************************************************************************************************\n")
        self.PROMPT_FILE.write(
            "*********************************************************************************************************************************************************\n")
        self.single_prompt_tokens = 0
        self.single_completion_tokens = 0
