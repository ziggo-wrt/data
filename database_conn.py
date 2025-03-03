## Necessary library for connect database
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datasets_cleansing import trans_df

## Database configuration
db_host = ""      # Input your host
db_port =         # Input your port  
db_user = ""      # Input your user
db_password =     # Input your password
db_scmaname = "frauds_db"
db_tablname = "transactions"
data_insert = pd.DataFrame(trans_df)

## Database connection (MySQL)
def conn_engine(host, port, username, password, database):
    engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")
    try:
        print("\nProcess: database connection ...")
        engine.connect()
    except SQLAlchemyError as errors:
        print(f"Connect status -> error: {errors}")
        return None
    except Exception as e:
        print(f"Connect status -> message error: {e}")
        return None
    else:
        print("Connect status -> successful")
        return engine

## Insert data to database (MySQL)
def insert_data(dataframe: pd.DataFrame, table: str):
    conn_database = conn_engine(db_host, db_port, db_user, db_password, db_scmaname)
    if conn_database is None:
        print("Please!! verify database configuration again")
    else:
        try:
            print("\nProcess: Insert data to database ...")
            print(f"Insert status -> data dimension input: {dataframe.shape}")
            dataframe.to_sql(table, con=conn_database, if_exists="append", index=False)
        except Exception as e:
            print(f"Insert status -> message error: {e} ...")
        else:
            print("Insert status -> successful")

insert_data(data_insert, db_tablname)
