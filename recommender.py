import psycopg2
import operator
import time
from collections import Counter
from scipy.stats import pearsonr
from util import normalize_values, get_venue_categories, create_preference_list, normalize_rating, get_predictor_max_ratings
from db_identifiers import db_identifiers

# TODO database connection as command line or sth to share code
# TODO maybe refactor to numpy?
# TODO maybe remove everything with just 1 checkin for performance, alternative below a threshold, e.g. 5
# TODO think about num of predictors, if low single predictors have huge influence, because they may be the only one with a rating, if high low correlation users influence decision


def get_pois_in_radius(lat, lon, radius, db_conn):
	cur = db_conn.cursor()
	sql = f"SELECT * \
            FROM {db_identifiers['poi_table']} \
            WHERE ST_DWithin(geom, ST_GeogFromText('POINT({lon} {lat})'), {radius});"
	cur.execute(sql)
	res = cur.fetchall()
	cur.close()
	results = []
	for row in res:
		item = {'venue_id': row[0], "lat": row[1], 'lon': row[2], 'venue_category_name': row[3], 'country_code': row[4], 'geom': row[5]}
		results.append(item)
	return results


# query all users who checked in at a poi
def get_users_for_poi(venue_id, db_conn):
	cur = db_conn.cursor()
	sql = f"SELECT userid FROM {db_identifiers['user_checkins_per_venue_table']} WHERE venueid = '{venue_id}' "
	cur.execute(sql)
	res = cur.fetchall()
	cur.close()
	users = []
	for row in res:
		users.append(row[0])
	return users


# number of checkins between 0 and 110000 (excluding home with 322116)
# query preferences of user, those are all venues he checked in, with more checkins indicating higher preference
def get_preferences_of_user(user_id, db_conn):
	cur = db_conn.cursor()
	sql = f"SELECT venue_category_name, c \
		from {db_identifiers['user_venue_checkins_table']} \
		WHERE userid = '{user_id}' "
	cur.execute(sql)
	results = cur.fetchall()
	cur.close()
	# transform to dict,because its easier to work with
	user_preference = {'user_id': user_id}
	for result in results:
		user_preference[result[0]] = result[1]

	normalized_preference = normalize_values(user_preference)
	return normalized_preference


# calculate correlation using pearson coefficient
def calculate_correlation(user, reference_user):
	# TODO negative correlation?
	corr, _ = pearsonr(reference_user, user)
	# TODO pearsonr may return nan if dvision by zero occurs, this happens when the standart deviation in one of the vectors is 0 (which is the case, if all values are equal) maybe handle that
	return corr


# uses weighted average approach as proposed in https://realpython.com/build-recommendation-engine-collaborative-filtering/#how-to-calculate-the-ratings
def predict_rating(poi, predictors, max_ratings,  db_conn):
	cur = db_conn.cursor()
	venue_id = poi['venue_id']
	ratings = {}
	for user, corr in predictors.items():
		sql = f"select c from {db_identifiers['user_checkins_per_venue_table']} \
				where userid = '{user}' and venueid = '{venue_id}' "
		cur.execute(sql)
		res = cur.fetchone()
		if res is not None:
			ratings[user] = res[0]
		else:
			ratings[user] = 0
	cur.close()
	# normalize ratings
	normalized_ratings = {}
	for user, rating in ratings.items():
		normalized_ratings[user] = normalize_rating(rating, max_ratings[user])

	sum_1 = 0
	sum_2 = 0
	for user, rating in normalized_ratings.items():
		# ignore if user did not visit location, otherwise it would be counted as string dislike, which is not necessarily the case
		if rating == 0:
			continue
		# find correlation of user that gave rating (predictor)
		corr = predictors[user]
		sum_1 += (rating * corr)
		sum_2 += corr
	if sum_2 != 0:
		result = sum_1 / sum_2
	else:
		result = 0
	"""
	# outdated appraoch where 0 is considered really negative
	# calculation of weigted average
	sum_1 = 0
	for user, rating in normalized_ratings.items():
		# find correlation of user that gave rating (predictor)
		corr = predictors[user]
		sum_1 = sum_1 + (rating * corr)
	sum_2 = sum(predictors.values())
	result = sum_1 / sum_2
	"""
	return result



def get_poi_from_id(venueid, db_conn):
	cur = db_conn.cursor()
	sql = f"select * from {db_identifiers['poi_table']} where venueid = '{venueid}'"
	cur.execute(sql)
	res = cur.fetchone()
	return res


def recommend_poi(user_preference_list, lat, lon, radius):
	db_name = db_identifiers['poi_db']
	db_host = 'localhost'
	db_user = 'bop'
	db_pw = '****'
	db_conn = psycopg2.connect(f"dbname='{db_name}' host='{db_host}' user='{db_user}' password='{db_pw}'")

	start = time.time()
	venue_categories = get_venue_categories(db_conn)
	print('get_venue_categories ' + str(time.time() - start))

	start = time.time()
	nearby_pois = get_pois_in_radius(lat, lon, radius, db_conn)
	print('nearby pois ' + str(time.time() - start))

	# get user who voted (checked in) on pois in radius
	start = time.time()
	reference_users = []
	for poi in nearby_pois:
		users = get_users_for_poi(poi['venue_id'], db_conn)
		if users is not None:
			reference_users = reference_users + users
	print('get users for poi ' + str(time.time() - start) + 'for num pois: ' + str(len(nearby_pois)))

	# preference matrix contains preferences of users. each row is a dict with userid and preferences
	# preferences is array with rating for each possible venue type
	preferences_all = {}

	start = time.time()
	for user in reference_users:
		user_preferences = get_preferences_of_user(user, db_conn)
		preferences_all[user] = create_preference_list(user_preferences, venue_categories)
	print('get preferences of user ' + str(time.time() - start) + 'for num users: ' + str(len(reference_users)))

	correlations = {}
	start = time.time()
	for user, pref in preferences_all.items():
		corr = calculate_correlation(user_preference_list, pref)
		correlations[user] = corr
	print('calculate correlations ' + str(time.time() - start) + 'for num prefs: ' + str(len(reference_users)))

	# only consider top similar users
	sorted_correlations = sorted(correlations.items(), key=operator.itemgetter(1), reverse=True)
	num_predictors = 5
	if len(sorted_correlations) > num_predictors:
		predictors = dict(sorted_correlations[:num_predictors])
	else:
		predictors = dict(sorted_correlations)
	print("predictors: " + str(predictors))
	# needed for normalization
	start = time.time()
	max_ratings = get_predictor_max_ratings(predictors, db_conn)
	print('get max ratings ' + str(time.time() - start) + 'for num nearby pois: ' + str(len(nearby_pois)))

	poi_ratings = {}
	start = time.time()
	for poi in nearby_pois:
		poi_ratings[poi['venue_id']] = predict_rating(poi, predictors, max_ratings, db_conn)
	print('predict rating ' + str(time.time() - start) + 'for num nearby pois: ' + str(len(nearby_pois)))

	# return three top recommendations
	counter = Counter(poi_ratings)
	best_rated = counter.most_common(5)
	recommended_pois = []
	for item in best_rated:
		poi_data = get_poi_from_id(item[0], db_conn)
		poi = {'venueid': poi_data[0],
			   'lat': poi_data[1],
			   'lon': poi_data[2],
			   'venue_category_name': poi_data[3],
			   'rating': item[1]}
		recommended_pois.append(poi)

	db_conn.close()
	return recommended_pois

