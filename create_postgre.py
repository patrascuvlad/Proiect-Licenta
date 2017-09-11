import time
from utils_postgre import create_user_tables_postgresql
import sys

if __name__ == "__main__":
  create_user_tables_postgresql(sys.argv[1])
