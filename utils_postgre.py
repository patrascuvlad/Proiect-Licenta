import time
import sys
import csv
import psycopg2
import ast
from datetime import datetime
from text import process_text
from queries_postgre import *

columns = ['_id', 'author', 'age', 'gender', 'geoLocation', 'rawText', 'date']
insert_fact_query = "INSERT INTO twitter (id_doc, count, tf, id_word, id_date, id_author, id_location) VALUES (%s, %s, %s, %s, %s, %s, %s)"
insert_date_query = "INSERT INTO date (id, date) VALUES (%s, %s)"
insert_doc_query = "INSERT INTO doc (id, raw_text) VALUES (%s, %s)"
insert_location_query = "INSERT INTO location (id, x, y) VALUES (%s, %s, %s)"
insert_author_query = "INSERT INTO author (id, age, gender) VALUES (%s, %s, %s)"
insert_word_query = "INSERT INTO word (id, word) VALUES (%s, %s)"

insert_fact_query_head = "INSERT INTO twitter_"
insert_fact_query_tail = " (id_doc, count, tf, id_word, id_date, id_author, id_location) VALUES (%s, %s, %s, %s, %s, %s, %s)"
insert_date_query_head = "INSERT INTO date_"
insert_date_query_tail = " (id, date) VALUES (%s, %s)"
insert_doc_query_head = "INSERT INTO doc_"
insert_doc_query_tail = " (id, raw_text) VALUES (%s, %s)"
insert_location_query_head = "INSERT INTO location_"
insert_location_query_tail = " (id, x, y) VALUES (%s, %s, %s)"
insert_author_query_head = "INSERT INTO author_"
insert_author_query_tail = " (id, age, gender) VALUES (%s, %s, %s)"
insert_word_query_head = "INSERT INTO word_"
insert_word_query_tail = " (id, word) VALUES (%s, %s)"

def get_create_twitter_query(username):
  return """CREATE TABLE twitter_%(username)s(
      id_doc character varying(20) NOT NULL,
      count integer NOT NULL,
      tf real NOT NULL,
      id_word integer NOT NULL,
      id_date integer NOT NULL,
      id_author character varying(20) NOT NULL,
      id_location integer NOT NULL,
      CONSTRAINT twitter_%(username)s_pkey PRIMARY KEY (id_doc, count, tf, id_word),
      CONSTRAINT twitter_%(username)s_id_author_fkey FOREIGN KEY (id_author) REFERENCES author_%(username)s(id),
      CONSTRAINT twitter_%(username)s_id_date_fkey FOREIGN KEY (id_date) REFERENCES date_%(username)s(id),
      CONSTRAINT twitter_%(username)s_id_doc_fkey FOREIGN KEY (id_doc) REFERENCES doc_%(username)s(id),
      CONSTRAINT twitter_%(username)s_id_location_fkey FOREIGN KEY (id_location) REFERENCES location_%(username)s(id),
      CONSTRAINT twitter_%(username)s_id_word_fkey FOREIGN KEY (id_word) REFERENCES word_%(username)s(id)
    );
  """ % {'username': username}

def get_create_author_query(username):
  return """CREATE TABLE author_%(username)s(
      id character varying(20) NOT NULL,
      age integer,
      gender character varying(10),
      CONSTRAINT author_%(username)s_pkey PRIMARY KEY (id)
    );
  """ % {'username': username}
def get_create_date_query(username):
  return """CREATE TABLE date_%(username)s(
      id integer NOT NULL,
      date timestamp without time zone,
      CONSTRAINT date_%(username)s_pkey PRIMARY KEY (id)
    );
  """ % {'username': username}
def get_create_doc_query(username):
  return """CREATE TABLE doc_%(username)s(
      id character varying(20) NOT NULL,
      raw_text text,
      CONSTRAINT doc_%(username)s_pkey PRIMARY KEY (id)
    );
  """ % {'username': username}
def get_create_location_query(username):
  return """CREATE TABLE location_%(username)s(
      id integer NOT NULL,
      x integer,
      y integer,
      CONSTRAINT location_%(username)s_pkey PRIMARY KEY (id)
    );
  """ % {'username': username}
def get_create_word_query(username):
  return """CREATE TABLE word_%(username)s(
      id integer NOT NULL,
      word text,
      CONSTRAINT word_%(username)s_pkey PRIMARY KEY (id)
    );
  """ % {'username': username}

def connect_postgresql():
  conn = psycopg2.connect("host='localhost' dbname='postgres' user='postgres' password='password'")
  return conn

# LOCK FUNCTION (no more changes)
def populate_postgresql():
  csv.field_size_limit(sys.maxsize)
  conn = connect_postgresql()
  cursor = conn.cursor()
  cursor.execute("TRUNCATE twitter, author, date, doc, location, word")
  author_dim = {}
  word_dim = {}
  location_dim = {}
  doc_dim = {}
  date_dim = {}
  with open('twitter.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    header = reader.next()
    for row in reader:
      facts, author_dim, word_dim, location_dim, doc_dim, date_dim = process_postgresql_row(cursor, row, author_dim, word_dim, location_dim, doc_dim, date_dim)
      conn.commit()
      for fact in facts:
        cursor.execute(insert_fact_query, (fact["id_doc"], fact["count"], fact["tf"], fact["id_word"], fact["id_date"], fact["id_author"], fact["id_location"]))

  conn.commit()
  conn.close()

def file_populate_postgresql(username, filename):
  csv.field_size_limit(sys.maxsize)
  conn = connect_postgresql()
  cursor = conn.cursor()
  cursor.execute("TRUNCATE twitter_%(username)s, author_%(username)s, date_%(username)s, doc_%(username)s, location_%(username)s, word_%(username)s" % {'username': username})
  author_dim = {}
  word_dim = {}
  location_dim = {}
  doc_dim = {}
  date_dim = {}
  with open('uploads/' + filename, 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    header = reader.next()
    for row in reader:
      facts, author_dim, word_dim, location_dim, doc_dim, date_dim = file_process_postgresql_row(username, cursor, row, author_dim, word_dim, location_dim, doc_dim, date_dim)
      conn.commit()
      for fact in facts:
        cursor.execute(insert_fact_query_head + username + insert_fact_query_tail, (fact["id_doc"], fact["count"], fact["tf"], fact["id_word"], fact["id_date"], fact["id_author"], fact["id_location"]))

  conn.commit()
  conn.close()

def drop_user_tables_postgresql(username):
  conn = connect_postgresql()
  cursor = conn.cursor()
  cursor.execute("DROP TABLE twitter_" + username)
  cursor.execute("DROP TABLE author_" + username)
  cursor.execute("DROP TABLE date_" + username)
  cursor.execute("DROP TABLE doc_" + username)
  cursor.execute("DROP TABLE location_" + username)
  cursor.execute("DROP TABLE word_" + username)
  conn.commit()
  conn.close()


def create_user_tables_postgresql(username):
  if username != "admin":
    conn = connect_postgresql()
    cursor = conn.cursor()
    cursor.execute(get_create_author_query(username))
    cursor.execute(get_create_date_query(username))
    cursor.execute(get_create_doc_query(username))
    cursor.execute(get_create_location_query(username))
    cursor.execute(get_create_word_query(username))
    conn.commit()
    cursor.execute(get_create_twitter_query(username))
    conn.commit()
    conn.close()

# process row for cassandradb function
def process_postgresql_row(cursor, row, author_dim, word_dim, location_dim, doc_dim, date_dim):
  facts = []

  # LOCK SECTION (no more changes)
  author_id = row[columns.index("author")]
  if author_id not in author_dim:
    author_dim[author_id] = {}
    author_dim[author_id]["age"] = int(row[columns.index("age")])
    author_dim[author_id]["gender"] = row[columns.index("gender")]
    cursor.execute(insert_author_query, (author_id, author_dim[author_id]["age"], author_dim[author_id]["gender"]))

  location = row[columns.index("geoLocation")]
  if location not in location_dim:
    location_arr = ast.literal_eval(location)
    location_dim[location] = {
      'id': len(location_dim) + 1,
      'x': int(location_arr[0]),
      'y': int(location_arr[1])
    }
    cursor.execute(insert_location_query, (location_dim[location]["id"], location_dim[location]["x"], location_dim[location]["y"]))

  doc_id = row[columns.index("_id")]
  if doc_id not in doc_dim:
    doc_dim[doc_id] = row[columns.index("rawText")]
    length = len(doc_dim[doc_id])
    cursor.execute(insert_doc_query, (doc_id, doc_dim[doc_id]))

  date = datetime.strptime(row[columns.index("date")], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M")
  if date not in date_dim:
    date_dim[date] = len(date_dim) + 1
    cursor.execute(insert_date_query, (date_dim[date], date))
  # UNLOCK SECTION

  # LOCK SECTION (no more changes)
  words = process_text(row[columns.index("rawText")])
  for word in words:
    if word["word"] not in word_dim:
      word_dim[word["word"]] = len(word_dim) + 1
      cursor.execute(insert_word_query, (word_dim[word["word"]], word["word"]))
    facts.append({
      'id_doc': doc_id,
      'count': word["count"],
      'tf': word["tf"],
      'id_word': word_dim[word["word"]],
      'id_date': date_dim[date],
      'id_author': author_id,
      'id_location': location_dim[location]["id"]
    })
  # UNLOCK SECTION

  return facts, author_dim, word_dim, location_dim, doc_dim, date_dim

# process row for cassandradb function
def file_process_postgresql_row(username, cursor, row, author_dim, word_dim, location_dim, doc_dim, date_dim):
  facts = []

  # LOCK SECTION (no more changes)
  author_id = row[columns.index("author")]
  if author_id not in author_dim:
    author_dim[author_id] = {}
    author_dim[author_id]["age"] = int(row[columns.index("age")])
    author_dim[author_id]["gender"] = row[columns.index("gender")]
    cursor.execute(insert_author_query_head + username + insert_author_query_tail, (author_id, author_dim[author_id]["age"], author_dim[author_id]["gender"]))

  location = row[columns.index("geoLocation")]
  if location not in location_dim:
    location_arr = ast.literal_eval(location)
    location_dim[location] = {
      'id': len(location_dim) + 1,
      'x': int(location_arr[0]),
      'y': int(location_arr[1])
    }
    cursor.execute(insert_location_query_head + username + insert_location_query_tail, (location_dim[location]["id"], location_dim[location]["x"], location_dim[location]["y"]))

  doc_id = row[columns.index("_id")]
  if doc_id not in doc_dim:
    doc_dim[doc_id] = row[columns.index("rawText")]
    length = len(doc_dim[doc_id])
    cursor.execute(insert_doc_query_head + username + insert_doc_query_tail, (doc_id, doc_dim[doc_id]))

  date = datetime.strptime(row[columns.index("date")], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M")
  if date not in date_dim:
    date_dim[date] = len(date_dim) + 1
    cursor.execute(insert_date_query_head + username + insert_date_query_tail, (date_dim[date], date))
  # UNLOCK SECTION

  # LOCK SECTION (no more changes)
  words = process_text(row[columns.index("rawText")])
  for word in words:
    if word["word"] not in word_dim:
      word_dim[word["word"]] = len(word_dim) + 1
      cursor.execute(insert_word_query_head + username + insert_word_query_tail, (word_dim[word["word"]], word["word"]))
    facts.append({
      'id_doc': doc_id,
      'count': word["count"],
      'tf': word["tf"],
      'id_word': word_dim[word["word"]],
      'id_date': date_dim[date],
      'id_author': author_id,
      'id_location': location_dim[location]["id"]
    })
  # UNLOCK SECTION

  return facts, author_dim, word_dim, location_dim, doc_dim, date_dim

def search_postgresql(username, selectBy, groupBy):
  start = time.time()
  conn = connect_postgresql()
  end = time.time()

  connectTime = end - start
  print "POSTGRE connected to db in ", connectTime

  joinWord = 'word' in groupBy
  joinLocation = 'x' in groupBy or 'y' in groupBy
  joinAuthor = 'gender' in groupBy or 'age' in groupBy
  joinDate = 'date' in groupBy

  cursor = conn.cursor()
  whereQuery = ''
  query = 'select count(*) from (select distinct on (twitter_' + username + '.id_doc) * from twitter_' + username

  if 'words' in selectBy:
    whereQuery += ' and ' +  query_one_of_words_postgresql(selectBy['words'].split())
    joinWord = True
  if 'location.x' in selectBy and 'location.y' in selectBy:
    whereQuery += ' and ' + query_location_postgresql(selectBy['location.x'], selectBy['location.y'])
    joinLocation = True
  elif 'location.x' in selectBy:
    whereQuery += ' and ' + query_location_x_postgresql(selectBy['location.x'])
    joinLocation = True
  elif 'location.y' in selectBy:
    whereQuery += ' and ' + query_location_y_postgresql(selectBy['location.y'])
    joinLocation = True

  if 'author.gender' in selectBy:
    whereQuery += ' and ' + query_gender_postgresql(selectBy['author.gender'])
    joinAuthor = True

  if 'author.age.min' in selectBy and 'author.age.max' in selectBy:
    whereQuery += ' and ' + query_age_between_postgresql(selectBy['author.age.min'], selectBy['author.age.max'])
    joinAuthor = True
  elif 'author.age.min' in selectBy:
    whereQuery += ' and ' + query_age_gte_postgresql(selectBy['author.age.min'])
    joinAuthor = True
  elif 'author.age.max' in selectBy:
    whereQuery += ' and ' + query_age_lte_postgresql(selectBy['author.age.max'])
    joinAuthor = True

  if 'date.start' in selectBy and 'date.stop' in selectBy:
    whereQuery += ' and ' + query_date_between_postgresql(selectBy['date.start'], selectBy['date.stop'])
    joinDate = True
  elif 'date.start' in selectBy:
    whereQuery += ' and ' + query_date_gte_postgresql(selectBy['date.start'])
    joinDate = True
  elif 'date.stop' in selectBy:
    whereQuery += ' and ' + query_date_lte_postgresql(selectBy['date.stop'])
    joinDate = True

  if joinWord:
    query += ' ' + query_join_word_postgresqp(username)
  if joinLocation:
    query += ' ' + query_join_location_postgresqp(username)
  if joinAuthor:
    query += ' ' + query_join_author_postgresqp(username)
  if joinDate:
    query += ' ' + query_join_date_postgresqp(username)
  if whereQuery != '':
    query += ' where' + whereQuery[4:]
  query += ') as temp'
  if groupBy != '':
    query += ' group by' + groupBy

  avgTime = 0
  print "POSTGRE query: ", query
  print "POSTGRE aggregating......."

  # Test 1
  start = time.time()
  cursor.execute(query)
  end = time.time()

  filterTime = end - start
  avgTime += filterTime
  # print "POSTGRE query executed in ", filterTime

  # Test 2
  start = time.time()
  cursor.execute(query)
  end = time.time()

  filterTime = end - start
  avgTime += filterTime
  # print "POSTGRE query executed in ", filterTime

  # Test 3
  start = time.time()
  cursor.execute(query)
  end = time.time()

  filterTime = end - start
  avgTime += filterTime
  # print "POSTGRE query executed in ", filterTime

  # Test 4
  start = time.time()
  cursor.execute(query)
  end = time.time()

  filterTime = end - start
  avgTime += filterTime
  # print "POSTGRE query executed in ", filterTime

  # Test 5
  start = time.time()
  cursor.execute(query)
  end = time.time()

  filterTime = end - start
  avgTime += filterTime
  # print "POSTGRE query executed in ", filterTime

  print cursor.fetchall()
  conn.close()
  avgTime /= 5
  print "POSTGRE avg time: ", avgTime
  print "--------------------------------------------------"
  return connectTime, avgTime
