# DB-Centric NL2SQL

### Environment

1. Config your local environment.

   ```bash
    $ conda create -n db4nl2sql python=3.12
    $ conda activate db4nl2sql
    $ pip3 install -r requirements.txt
   ```

2. Edit the `.env` file for project setting. 

   ```bash
    ### .env file
    # Base Directory Setting
    BASE_DIR="The directory you downloaded the code. (i.e. /Users/DB4NL2SQL/)"
   
    # OpenAI Setting
    OPENAI_API_KEY_0="YOUR API KEY 1"
    OPENAI_API_KEY_1="YOUR API KEY 2"
    OPENAI_API_KEY_2="YOUR API KEY 3"
    OPENAI_API_KEY_3="YOUR API KEY 4" 
   ```

   ```bash
   $ source .env
   ```

3. Edit the `run_exp.sh` file for experiment setting.

   ```bash
    logging=0                        # 0 if False, 1 if True, log the prompt
    model='gpt-4o'                   # gpt-4o
    mode='baseline'                  # baseline, baseline-w-desc, view
    num_cpus=4                       # # of cpus. Default 4. 
    result_file='predict_dev.json'   # The name of the result file. Should be json format.

    echo '''starting experiment...'''
    python3 ./src/nl2sql.py --logging ${logging} --model ${model} --mode ${mode} \
    --num_cpus ${num_cpus} --result_file ${result_file} --max_refine_num ${max_refine_num}
   ```

### Data Preparation

The data folder currently contains the dev data set. It should include the following items.<br/>

To experiment with the dev set, download the "dev_databases" folder from the following Google Drive directory, then replace the "dev_databases" folder within your project: [Google Drive link](https://drive.google.com/file/d/15d7Gk0uimCSdPWninA2agFCX7Yp-DCi2/view?usp=drive_link) 


To change the dev data set to test data set, `dev_databases`, `dev_table.json`, `dev_tied_append.json`, `dev.json`, `dev_gold.sql`, `column_meaning.json` files should be replaced.


### Project Structure

```
./
├── data/
│   ├── coldb/
│   ├── coldescdb/
│   ├── dev_databases/
│   │   ├── california_schools/...
│   │   └── ...
│   ├── errordb/
│   │   └── chroma.sqlite3
│   ├── fewshotdb/
│   │   └── shotdb.csv
│   ├── refinedb/
│   │   └── refine.json
│   ├── dev_tables.json
│   ├── dev_tied_append.json
│   ├── dev.json
│   └── dev_gold.sql
├── exp_result/
│   │   └── column_meaning.json
├── run/
│   ├── run_evaluation_ves.sh
│   ├── run_evaluation.sh
│   └── run_exp.sh
├── src/
|   ├── agent/
│   │   └── ErrorChecker.py
│   │   └── SchemaLinker.py
│   │   └── SQLGenerator.py
|   ├── log/
|   ├── util/
│   │   └── const.py
│   │   └── preprocessor.py
│   │   └── logger.py
│   │   └── util.py
│   ├── evaluation.py
│   └── nl2sql_bird.py
├── .env
├── README.md
└── requirements.txt
```


### How to Run

1. Run the `run_exp.sh` file.
   ```bash
   $ ./run/run_exp.sh
   ```
2. After the experiment, the result will be stored in `exp_result` directory in a json format.
   ```
   ./
   ├── ...
   ├── exp_result/
   │   ├── pred_dev.json
   ├── ...
   ```
3. To evaluate the result, run the following scripts. (The evaluation code scripts are from Bird-bench.)
   ```bash
   $ chmod 777 ./run/run_evaluation.sh     # for permission
   $ chmod 777 ./run/run_evaluation_ves.sh # for permission
   $ ./run/run_evaluation.sh     # for EX, VES evaluation
   $ ./run/run_evaluation_ves.sh # for VES evaluation
   ```

