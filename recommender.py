import psycopg2
import operator
from scipy.stats import pearsonr

# TODO normalize preference values, maybe read references papers on checkin based POI
# TODO database connection as command line or sth to share code
# TODO close database connections
# TODO maybe refactor to numpy?
# TODO maybe remove everything with just 1 checkin for performance, alternative below a threshold, e.g. 5
# TODO if a user has 500 checkins it weights much higher than 20. we need to normalize this somehow

def get_pois_in_radius(lat, lon, radius):
	cur = db_conn.cursor()
	sql = f'SELECT * \
            FROM pois \
            WHERE ST_DWithin(geom, ST_GeogFromText(\'POINT({lon} {lat})\'), {radius});'
	cur.execute(sql)
	res = cur.fetchall()
	results = []
	for row in res:
		item = {'venue_id': row[0], "lat": row[1], 'lon': row[2], 'venue_category_name': row[3], 'country_code': row[4], 'geom': row[5]}
		results.append(item)
	return results


# query all users who checked in at a poi
def get_users_for_poi(venue_id):
	cur = db_conn.cursor()
	sql = f'SELECT userid FROM user_checkins_per_venue WHERE venueid = \'{venue_id}\' '
	cur.execute(sql)
	res = cur.fetchall()
	users = []
	for row in res:
		users.append(row[0])
	return users


# apply min/max normalization
def normalize_values(user_preference):
	# remove user id string from values
	values = list(filter(lambda i: not (type(i) is str), user_preference.values()))

	min_value = 0
	max_value = max(values)
	for key, value in user_preference.items():
		if key is not 'user_id':
			user_preference[key] = (value - min_value) / (max_value - min_value)
	return user_preference

# number of checkins between 0 and 110000 (excluding home with 322116)
# query preferences of user, those are all venues he checked in, with more checkins indicating higher preference
def get_preferences_of_user(user_id):
	cur = db_conn.cursor()
	sql = f'SELECT venue_category_name, c \
		from user_venue_checkins \
		WHERE userid = \'{user_id}\' '
	cur.execute(sql)
	results = cur.fetchall()
	# transform to dict,because its easier to work with
	user_preference = {'user_id': user_id}
	for result in results:
		user_preference[result[0]] = result[1]

	normalized_preference = normalize_values(user_preference)
	return normalized_preference

# returns list of all possible venue categories
def get_venue_categories():
	cur = db_conn.cursor()
	sql = f'SELECT venue_category_name from pois group by venue_category_name'
	cur.execute(sql)
	results = cur.fetchall()

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


# calculate correlation using pearson coefficient
def calculate_correlation(user, reference_user):
	# TODO negative correlation?
	corr, _ = pearsonr(reference_user, user)
	# TODO pearsonr may return nan if dvision by zero occurs, this happens when the standart deviation in one of the vectors is 0 (which is the case, if all values are equal) maybe handle that
	return corr


# uses weighted average approach as proposed in https://realpython.com/build-recommendation-engine-collaborative-filtering/#how-to-calculate-the-ratings
def predict_rating(poi, predictors):
	cur = db_conn.cursor()
	venue_id = poi['venue_id']
	ratings = {}
	for user, corr in predictors.items():
		sql = f'select c from user_checkins_per_venue \
				where userid = \'{user}\' and venueid = \'{venue_id}\' '
		cur.execute(sql)
		res = cur.fetchone()
		if res is not None:
			ratings[user] = res[0]
		else:
			ratings[user] = 0

	# calculation of weigted average
	sum_1 = 0
	for user, rating in ratings.items():
		# find correlation of user that gave rating (redictor)
		corr = predictors[user]
		sum_1 = sum_1 + (rating * corr)
	sum_2 = sum(predictors.values())
	return sum_1 / sum_2


if __name__ == '__main__':
	db_name = 'poi_db'
	db_host = 'localhost'
	db_user = 'bop'
	db_pw = '****'
	db_conn = psycopg2.connect(f"dbname='{db_name}' host='{db_host}' user='{db_user}' password='{db_pw}'")

	venue_categories = get_venue_categories()

	# create dummy user until we have our own
	dummy_id = '86747'
	dummy_user_pref = get_preferences_of_user(dummy_id)
	dummy_user_preference = create_preference_list(dummy_user_pref, venue_categories)

	lon = 8.511101
	lat = 47.377845
	# radius is in metres
	radius = 100

	nearby_pois = get_pois_in_radius(lat, lon, radius)

	# get user who voted (checked in) on pois in radius
	reference_users = []
	for poi in nearby_pois:
		users = get_users_for_poi(poi['venue_id']) 
		if users is not None:
			reference_users = reference_users + users

	# user_precomputed = ['222253', '207259', '56605', '701', '8393', '86747', '9689', '111180', '207259', '86747', '9689', '166487']
	# reference_users = user_precomputed

	# preference matrix contains preferences of users. each row is a dict with userid and preferences
	# preferences is array with rating for each possible venue type
	preferences_all = {}

	for user in reference_users:
		user_preferences = get_preferences_of_user(user)
		preferences_all[user] = create_preference_list(user_preferences, venue_categories)

	correlations = {}
	for user, pref in preferences_all.items():
		corr = calculate_correlation(dummy_user_preference, pref)
		correlations[user] = corr

	# only consider top 5 similar users
	sorted_correlations = sorted(correlations.items(), key=operator.itemgetter(1),reverse=True)
	predictors = dict(sorted_correlations[:5])

	poi_ratings = {}
	for poi in nearby_pois:
		poi_ratings[poi['venue_id']] = predict_rating(poi, predictors)

	print(poi_ratings)