import time
from utils_mongo import populate_mongodb

if __name__ == "__main__":
  start = time.time()
  populate_mongodb()
  end = time.time()
  print "populate mongo time", (end - start)
