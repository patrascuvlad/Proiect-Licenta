import time
import sys
import csv
import pymongo
import ast
from datetime import datetime
from text import process_text
from queries_mongo import *
from werkzeug.security import check_password_hash

columns = ['_id', 'author', 'age', 'gender', 'geoLocation', 'rawText', 'date']

def connect_mongodb():
  client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
  db = client['upb']
  table = db['twitter_admin']
  return client, table

def connect_history_db(username):
  client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
  db = client['upb']
  table = db['history_' + username]
  return client, table

def connect_user_db():
  client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
  db = client['upb']
  table = db['users']
  return client, table

def create_table(db, tableName):
    db[tableName].insert({})
    db[tableName].remove()

def create_user_tables_mongodb(db, username):
  if username != "admin":
    create_table(db, 'twitter_' + username)
    create_table(db, 'history_' + username)

def create_user(user):
  client, table = connect_user_db()
  username = user['username']
  userAlreadyExists = table.find({'username': username}).count() > 0;

  if not userAlreadyExists:
    table.insert(user)
    create_user_tables_mongodb(client['upb'], username)
    client.close()
    return True

  client.close()
  return False

def validate_user(user):
  client, table = connect_user_db()
  userObject = table.find_one({ 'username': user['username'] });
  client.close()

  if userObject:
    if check_password_hash(userObject['password'], user['password']):
      return { 'username': userObject['username'] }
  return None

def get_user(username):
  client, table = connect_user_db()
  userObject = table.find_one({ 'username': username })
  client.close()

  if userObject:
    return { 'username': username }
  return None

def get_users():
  client, table = connect_user_db()
  users = table.find({}, {'username': 1})
  listUsers = list(users)
  client.close()

  return listUsers

def save_user_history(username, times, docs):
  client, table = connect_history_db(username)
  docsFormated = []
  for doc in docs:
    docId = doc['_id']
    docObj = {}

    if 'words.word' in docId:
      docObj['word'] = docId['words.word']
    if 'location.x' in docId:
      docObj['x'] = docId['location.x']
    if 'location.y' in docId:
      docObj['y'] = docId['location.y']
    if 'author.gender' in docId:
      docObj['gender'] = docId['author.gender']
    if 'author.age' in docId:
      docObj['age'] = docId['author.age']
    if 'date' in docId:
      docObj['date'] = docId['date']

    docsFormated.append({
      'count': doc['count'],
      '_id': docObj
    })

  table.insert({
    '_id': datetime.now().strftime("%Y%m%d%H%M%S%f"),
    'connectTimes': times['connectTimes'],
    'filterTimes': times['filterTimes'],
    'docs': docsFormated,
    'date': datetime.utcnow()
  })
  client.close()

def get_user_history(username):
  client, table = connect_history_db(username)
  docs = table.find({}, {
    'docs': 1,
    'filterTimes': 1,
    'connectTimes': 1
  }).sort('date', -1)
  listDocs = list(docs)
  client.close()
  return listDocs

# LOCK FUNCTION (no more changes)
def populate_mongodb():
  csv.field_size_limit(sys.maxsize)
  client, table = connect_mongodb()
  table.remove()
  with open('twitter.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    header = reader.next()
    for row in reader:
      result = process_mongodb_row(row)
      table.insert(result, continue_on_error=True)
  client.close()

# process row for mongodb function
# LOCK FUNCTION (no more changes)
def process_mongodb_row(row):
  location = ast.literal_eval(row[columns.index("geoLocation")])
  result = {}
  result["_id"] = row[columns.index("_id")]
  result["rawText"] = row[columns.index("rawText")]
  result["author"] = {}
  result["author"]["id"] = row[columns.index("author")]
  result["author"]["age"] = int(row[columns.index("age")])
  result["author"]["gender"] = row[columns.index("gender")]
  result["location"] = {}
  result["location"]["x"] = int(location[0])
  result["location"]["y"] = int(location[1])
  result["date"] = datetime.strptime(row[columns.index("date")], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M")
  result["words"] = process_text(result["rawText"])
  return result

def search_mongodb(selectBy, groupBy):
  start = time.time()
  client, twitterDb = connect_mongodb()
  end = time.time()

  connectTime = end - start
  print "MONGO connected to db in ", connectTime

  query = {}

  if 'words' in selectBy:
    query.update(query_one_of_words_mongodb(selectBy['words'].split()))

  if 'location.x' in selectBy and 'location.y' in selectBy:
    query.update(query_location_mongodb(selectBy['location.x'], selectBy['location.y']))
  elif 'location.x' in selectBy:
    query.update(query_location_x_mongodb(selectBy['location.x']))
  elif 'location.y' in selectBy:
    query.update(query_location_y_mongodb(selectBy['location.y']))

  if 'author.gender' in selectBy:
    query.update(query_gender_mongodb(selectBy['author.gender']))

  if 'author.age.min' in selectBy and 'author.age.max' in selectBy:
    query.update(query_age_between_mongodb(selectBy['author.age.min'], selectBy['author.age.max']))
  elif 'author.age.min' in selectBy:
    query.update(query_age_gte_mongodb(selectBy['author.age.min']))
  elif 'author.age.max' in selectBy:
    query.update(query_age_lte_mongodb(selectBy['author.age.max']))

  if 'date.start' in selectBy and 'date.stop' in selectBy:
    query.update(query_date_between_mongodb(selectBy['date.start'], selectBy['date.stop']))
  elif 'date.start' in selectBy:
    query.update(query_date_gte_mongodb(selectBy['date.start']))
  elif 'date.stop' in selectBy:
    query.update(query_date_lte_mongodb(selectBy['date.stop']))

  avgTime = 0
  print "MONGO group: ", groupBy, " MONGO query ", query
  print "MONGO aggregating......."

  # Test 1
  start = time.time()
  docs = twitterDb.aggregate([
    { "$match": query },
    {
      "$group": {
        "_id": groupBy,
        "count": { "$sum": 1 }
      }
    }
  ])
  end = time.time()

  filterTime = end - start
  avgTime += filterTime
  # print "MONGO query executed in ", filterTime

  # Test 2
  start = time.time()
  docs = twitterDb.aggregate([
    { "$match": query },
    {
      "$group": {
        "_id": groupBy,
        "count": { "$sum": 1 }
      }
    }
  ])
  end = time.time()

  filterTime = end - start
  avgTime += filterTime
  # print "MONGO query executed in ", filterTime

  # Test 3
  start = time.time()
  docs = twitterDb.aggregate([
    { "$match": query },
    {
      "$group": {
        "_id": groupBy,
        "count": { "$sum": 1 }
      }
    }
  ])
  end = time.time()

  filterTime = end - start
  avgTime += filterTime
  # print "MONGO query executed in ", filterTime

  # Test 4
  start = time.time()
  docs = twitterDb.aggregate([
    { "$match": query },
    {
      "$group": {
        "_id": groupBy,
        "count": { "$sum": 1 }
      }
    }
  ])
  end = time.time()

  filterTime = end - start
  avgTime += filterTime
  # print "MONGO query executed in ", filterTime

  # Test 5
  start = time.time()
  docs = twitterDb.aggregate([
    { "$match": query },
    {
      "$group": {
        "_id": groupBy,
        "count": { "$sum": 1 }
      }
    }
  ])
  end = time.time()

  filterTime = end - start
  avgTime += filterTime
  # print "MONGO query executed in ", filterTime

  listDocs = list(docs)
  print listDocs
  client.close()
  avgTime /= 5
  print "MONGO avg time: ", avgTime
  print "--------------------------------------------------"
  return connectTime, avgTime, listDocs
