from util import create_preference_list, get_venue_categories, normalize_values
from recommender import recommend_poi
from db_identifiers import db_identifiers
import psycopg2
import time


# TODO possible poi in destination can be found smarter
def get_poi_for_dest(lat, lon):
    radius = 75
    conn = psycopg2.connect(f"dbname='{db_identifiers['poi_db']}' host='localhost' user='bop' password='****'")
    cur = conn.cursor()

    sql = f"SELECT * \
            FROM {db_identifiers['poi_table']} \
            WHERE ST_DWithin(geom, ST_GeogFromText('POINT({lon} {lat})'), \
                {radius});"

    cur.execute(sql)
    res = cur.fetchall()
    cur.close()
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
    # user 455 has 271 trips on 31 locations
    user_id = '455'
    lat_user = 52.5219184
    lon_user = 13.4132147
    recommendation_radius = 500

    db_name = 'gps_db'
    db_host = 'localhost'
    db_user = 'bop'
    db_pw = '****'
    db_conn_gps = psycopg2.connect(f"dbname='{db_name}' host='{db_host}' user='{db_user}' password='{db_pw}'")

    db_name = 'poi_db'
    db_conn_pois = psycopg2.connect(f"dbname='{db_name}' host='{db_host}' user='{db_user}' password='{db_pw}'")

    cur_gps = db_conn_gps.cursor()
    start = time.time()
    # get destination of trips
    proc_trips = db_identifiers['processed_trips_table']
    sessions = db_identifiers['sessions_table']
    sql = f"SELECT {proc_trips}.lat_end, {proc_trips}.lon_end, count(*) as c from {proc_trips} inner join {sessions} on \
            {proc_trips}.daily_user_id={sessions}.daily_user_id \
            where {sessions}.user_id='{user_id}' \
            group by {proc_trips}.lat_end, {proc_trips}.lon_end"
    cur_gps.execute(sql)
    results = cur_gps.fetchall()
    print('get destinations of trips ' + str(time.time() - start))
    cur_gps.close()

    user_preferences = {}
    start = time.time()
    for result in results:
        lat = result[0]
        lon = result[1]
        count = result[2]
        poi = get_poi_for_dest(lat, lon)
        if poi is not None:
            user_preferences[poi['venue_category_name']] = count
    print('poi for dest ' + str(time.time() - start) + 'for num dest: ' + str(len(results)))
    user_preferences_normalized = normalize_values(user_preferences)

    venue_categories = get_venue_categories(db_conn_pois)
    user_preference_list = create_preference_list(user_preferences_normalized, venue_categories)
    poi_ratings = recommend_poi(user_preference_list, lat_user, lon_user, recommendation_radius)
    print(poi_ratings)
    db_conn_gps.close()
    db_conn_pois.close()





