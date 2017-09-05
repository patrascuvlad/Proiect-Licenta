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

def query_join_word_postgresqp():
  return "join word on twitter.id_word=word.id"

def query_join_location_postgresqp():
  return "join location on twitter.id_location=location.id"

def query_join_author_postgresqp():
  return "join author on twitter.id_author=author.id"

def query_join_date_postgresqp():
  return "join date on twitter.id_date=date.id"

def query_join_doc_postgresqp():
  return "join doc on twitter.id_doc=doc.id"
