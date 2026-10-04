import logging
import uuid
from datetime import datetime
from src.utils.paths import pipe_logs_path

class LoadMonitor:

#Class constructor se ejecuta automaticamente cuando creo el objeto LoadMonitor, self apunta al objeto job_monitor, self Guarda el valor de process_name dentro del objeto actual.
    def __init__(self, process_name):
        self.run_id = str(uuid.uuid4())[:8]
        self.process_name = process_name
        self.start_time = None
        self.logger = self.get_logger()

    def start(self):
        self.start_time = datetime.now()

        self.logger.info(
            f"RUN_ID={self.run_id} | "
            f"PROCESS={self.process_name} | "
            f"STATUS=STARTED"
        )

    def success(self, records):
        end_time = datetime.now()

        duration = (
            end_time - self.start_time
        ).total_seconds()

        self.logger.info(
            f"RUN_ID={self.run_id} | "
            f"PROCESS={self.process_name} | "
            f"STATUS=SUCCESS | "
            f"RECORDS={records} | "
            f"DURATION={duration:.2f}s"
        )

    def failed(self, error):
        end_time = datetime.now()

        duration = (
            end_time - self.start_time
        ).total_seconds()

        self.logger.error(
            f"RUN_ID={self.run_id} | "
            f"PROCESS={self.process_name} | "
            f"STATUS=FAILED | "
            f"ERROR={type(error).__name__} | "
            f"MESSAGE={error} | "
            f"DURATION={duration:.2f}s"
        )
  
    def get_logger(self):
        print(pipe_logs_path)
        pipe_logs_path.mkdir(exist_ok=True)
        logger = logging.getLogger(self.process_name)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        if not logger.handlers:

            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)s | %(message)s'
            )

            # Archivo
            file_handler = logging.FileHandler(
                f"{pipe_logs_path}/{self.process_name}.log",
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)

            # Consola
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        return logger