# ubiquitious-system-project
POI Recommender System for the  Ubiquitous System Seminar (University of Coimbra, 2020)

The file recommender.py provides a first draft of the POI recommender system. It takes a user, a location (lat, lon) and a radius and outputs recommendations based on the users interest in the radius around the given location.


## Setup 
To make the correct database calls the database information need to be adjusted in line 127-131. 

The following tables need to exist:

* pois (contains venues from POI dataset)
* checkins (contains checkin data from POI dataset)
* user_checkins_per_venue (can be created running this query: CREATE TABLE user_checkins_per_venue AS
(SELECT userid, venueid, COUNT(*) as c 
		from checkins 
		group by userid, venue_category_name))
* user_venue_checkins (can be created running the query CREATE TABLE user_venue_checkins AS
(SELECT checkins.userid, pois.venue_category_name, COUNT(*) as c 
		from checkins INNER JOIN pois on pois.venueid = checkins.venueid 
		group by userid, venue_category_name)
)

## Parameters
* The system currently only supports recommendation for users from the poi dataset. a user can be chosen by entering its userid as dummy_id in line 136.
* latitude, longitude and radius can be entered in line 140-143.
