logging=1                   # 0 if False, 1 if True, log the prompt
model='gpt-4o'         # gpt-4o, gpt-4o-mini, llama3.1:8b
schema_mode='view' # baseline, baseline-w-desc, view
num_cpus=4                  # number of cpu (1~4). Default 4.
result_file='predict_dev.json' # name of the result file. Should be json format.

echo '''starting experiment...'''
python3 ./src/nl2sql_bird.py --logging ${logging} --model ${model} --schema_mode ${schema_mode} \
--num_cpus ${num_cpus} --result_file ${result_file} 

