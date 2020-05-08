from util import create_preference_list, get_venue_categories, normalize_values
from recommender import recommend_poi
import psycopg2
import time


# TODO possible poi in destination can be found smarter
def get_poi_for_dest(lat, lon):
    radius = 50
    conn = psycopg2.connect("dbname='poi_db' host='localhost' user='bop' password='****'")
    cur = conn.cursor()
    #sql = f'SELECT * FROM pois WHERE ST_Distance_Sphere(geom, ST_MakePoint({lat},{lon})) <= {radius}'

    sql = f'SELECT * \
            FROM pois \
            WHERE ST_DWithin(geom, ST_GeogFromText(\'POINT({lon} {lat})\'), \
                {radius});'

    cur.execute(sql)
    res = cur.fetchall()
    results = []
    for row in res:
        item = {'venue_id': row[0], "lon": row[1], 'lat': row[2], 'venue_category_name': row[3], 'country_code': row[4], 'geom': row[5]}
        results.append(item)
    # TODO atm we just use first result
    if not results == []:
        return results[0]
    else:
        return None


if __name__ == '__main__':
    # measure execution time
    start = time.time()
    # user 455 has 271 trips on 31 locations
    user_id = '455'
    lat_user = 47.377845
    lon_user = 8.511101
    recommendation_radius = 500

    db_name = 'gps_db'
    db_host = 'localhost'
    db_user = 'bop'
    db_pw = '****'
    db_conn_gps = psycopg2.connect(f"dbname='{db_name}' host='{db_host}' user='{db_user}' password='{db_pw}'")

    db_name = 'poi_db'
    db_conn_pois = psycopg2.connect(f"dbname='{db_name}' host='{db_host}' user='{db_user}' password='{db_pw}'")

    cur_gps = db_conn_gps.cursor()
    # get destination of trips
    sql = f'SELECT processed_trips.lat_end, processed_trips.lon_end, count(*) as c from processed_trips inner join sessions on \
            processed_trips.daily_user_id=sessions.daily_user_id \
            where sessions.user_id=\'{user_id}\' \
            group by processed_trips.lat_end, processed_trips.lon_end'
    cur_gps.execute(sql)
    results = cur_gps.fetchall()

    user_preferences = {}

    radius = 50
    for result in results:
        lat = result[0]
        lon = result[1]
        count = result[2]
        poi = get_poi_for_dest(lat, lon)
        if poi is not None:
            user_preferences[poi['venue_category_name']] = count
    user_preferences_normalized = normalize_values(user_preferences)

    venue_categories = get_venue_categories(db_conn_pois)
    user_preference_list = create_preference_list(user_preferences_normalized, venue_categories)
    poi_ratings = recommend_poi(user_preference_list, lat_user, lon_user, recommendation_radius)
    print(poi_ratings)

    print('time:' + str(time.time() - start))





