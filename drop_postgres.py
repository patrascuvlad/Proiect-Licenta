import time
from utils_postgre import drop_user_tables_postgresql
import sys

if __name__ == "__main__":
  drop_user_tables_postgresql(sys.argv[1])
