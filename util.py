from db_identifiers import db_identifiers

# apply min/max normalization
def normalize_values(user_preference):
    # remove user id string from values
    values = list(filter(lambda i: not (type(i) is str), user_preference.values()))

    min_value = 0
    max_value = max(values)
    for key, value in user_preference.items():
        if key is not 'user_id':
            user_preference[key] = value / max_value
    return user_preference


def get_predictor_max_ratings(predictors, db_conn):
    max_ratings = {}
    for predictor in predictors:
        sql = f"select c \
                from {db_identifiers['user_checkins_per_venue_table']} where userid = '{predictor}' \
                order by c desc "
        cur = db_conn.cursor()
        cur.execute(sql)
        max_ratings[predictor] = cur.fetchone()[0]
        cur.close()
    return max_ratings


def normalize_rating(rating, max_rating):
    return rating / max_rating


# returns list of all possible venue categories
def get_venue_categories(db_conn):
    cur = db_conn.cursor()
    sql = f"SELECT venue_category_name from {db_identifiers['poi_table']} group by venue_category_name"
    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    # transform from tupel to list of strings
    venue_categories = []
    for result in results:
        venue_categories.append(result[0])
    return venue_categories


def create_preference_list(user_preferences, venue_categories):
    preference_single = []
    for category in venue_categories:
        if category in user_preferences:
            preference_single.append(user_preferences[category])
        else:
            preference_single.append(0)
    return preference_single
