from dotenv import load_dotenv
import sys
import io
import logging
import pandas as pd
from src.extract.extract_api_earthquakes import extract_earthquakes


# Setup Structured Production Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DB_LOADER")

### Loads the .env file
load_dotenv()    


##Data Transformations

def earthquake_transform(df: pd.DataFrame) -> pd.DataFrame:
        
        df["longitude"] = df["longitude"].astype(float)
        df["latitude"] = df["latitude"].astype(float)
        df["depth"] = df["depth"].astype(float)
        df["time"] = pd.to_datetime(df["time"], unit="ms")

        return df

def dataframe_to_buffer(df):
    buffer = io.StringIO()

    df.to_csv(buffer,index=False,header=True)

    buffer.seek(0)

    return buffer