import psycopg2


def get_pois_in_radius(lat, lon, radius):
    conn = psycopg2.connect("dbname='poi_db' host='localhost' user='bop' password='****'")
    cur = conn.cursor()
    #sql = f'SELECT * FROM pois WHERE ST_Distance_Sphere(geom, ST_MakePoint({lat},{lon})) <= {radius}'

    sql = f'SELECT * \
            FROM pois \
            WHERE ST_DWithin(geom, ST_GeogFromText(\'POINT({lon} {lat})\'), \
                {radius});'
    #sql = f'SELECT * FROM pois WHERE ST_Distance_Sphere(geom, ST_MakePoint({lon},{lat})) <= {radius}'

    cur.execute(sql)
    res = cur.fetchall()
    results = []
    for row in res:
        item = {'venue_id': row[0], "lon": row[1], 'lat': row[2], 'venue_category_name': row[3], 'country_code': row[4], 'geom': row[5]}
        results.append(item)
    return results


if __name__ == '__main__':
    # user 455 has 271 trips on 31 locations
    user_id = '455'

    db_name = 'gps_db'
    db_host = 'localhost'
    db_user = 'bop'
    db_pw = '****'
    db_conn_gps = psycopg2.connect(f"dbname='{db_name}' host='{db_host}' user='{db_user}' password='{db_pw}'")

    db_name = 'poi_db'
    db_conn_pois = psycopg2.connect(f"dbname='{db_name}' host='{db_host}' user='{db_user}' password='{db_pw}'")

    cur_gps = db_conn_gps.cursor()
    sql = f'SELECT processed_trips.lat_end, processed_trips.lon_end, count(*) as c from processed_trips inner join sessions on \
            processed_trips.daily_user_id=sessions.daily_user_id \
            where sessions.user_id=\'{user_id}\' \
            group by processed_trips.lat_end, processed_trips.lon_end'
    cur_gps.execute(sql)
    results = cur_gps.fetchall()
    radius = 50
    for result in results:
        lat = result[0]
        lon = result[1]
        pois = get_pois_in_radius(lat, lon, radius)
        print(pois)



