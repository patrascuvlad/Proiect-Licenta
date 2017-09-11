def query_one_of_words_postgresql(words):
  return "word=any(array['" + "', '".join(words) + "'])"

def query_location_x_postgresql(x):
  return "x='" + str(x) + "'"

def query_location_y_postgresql(y):
  return "y='" + str(y) + "'"

def query_location_postgresql(x, y):
  return "x='" + str(x) + "' and y='" + str(y) + "'"

def query_gender_postgresql(gender):
  return "gender='" + gender + "'"

def query_age_gte_postgresql(age):
  return "age>='" + str(age) + "'"

def query_age_lte_postgresql(age):
  return "age<='" + str(age) + "'"

def query_age_between_postgresql(minAge, maxAge):
  return "age<='" + str(maxAge) + "' and age>='" + str(minAge) + "'"

def query_date_gte_postgresql(date):
  return "date>='" + date + "'"

def query_date_lte_postgresql(date):
  return "date<='" + date + "'"

def query_date_between_postgresql(minDate, maxDate):
  return "date<='" + maxDate + "' and date>='" + minDate + "'"

def query_join_word_postgresqp(username):
  return "join word_" + username + " on twitter_" + username + ".id_word=word_" + username + ".id"

def query_join_location_postgresqp(username):
  return "join location_" + username + " on twitter_" + username + ".id_location=location_" + username + ".id"

def query_join_author_postgresqp(username):
  return "join author_" + username + " on twitter_" + username + ".id_author=author_" + username + ".id"

def query_join_date_postgresqp(username):
  return "join date_" + username + " on twitter_" + username + ".id_date=date_" + username + ".id"

def query_join_doc_postgresqp(username):
  return "join doc_" + username + " on twitter_" + username + ".id_doc=doc_" + username + ".id"
