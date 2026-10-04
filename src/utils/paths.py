from pathlib import Path 

PROJECT_ROOT = Path(__file__).resolve().parents[2]

data_raw_path = PROJECT_ROOT/"data"/"raw"
data_clean_path = PROJECT_ROOT/"data"/"clean"
data_gold_path = PROJECT_ROOT/"data"/"gold"
pipe_logs_path = PROJECT_ROOT/"logs"/"pipeline"