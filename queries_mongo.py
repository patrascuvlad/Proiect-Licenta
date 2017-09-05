def query_one_of_words_mongodb(words):
  return {"words.word": {"$in": words}}

def query_location_x_mongodb(x):
  return {"location.x": int(x)}

def query_location_y_mongodb(y):
  return {"location.y": int(y)}

def query_location_mongodb(x, y):
  return {"location.x": int(x), "location.y": int(y)}

def query_gender_mongodb(gender):
  return {"author.gender": gender}

def query_age_gte_mongodb(age):
  return {"author.age": {"$gte": int(age)}}

def query_age_lte_mongodb(age):
  return {"author.age": {"$lte": int(age)}}

def query_age_between_mongodb(minAge, maxAge):
  return {"author.age": {"$gte": int(minAge), "$lte": int(maxAge)}}

def query_date_gte_mongodb(date):
  return {"date": {"$gte": date}}

def query_date_lte_mongodb(date):
  return {"date": {"$lte": date}}

def query_date_between_mongodb(minDate, maxDate):
  return {"date": {"$gte": minDate, "$lte": maxDate}}
