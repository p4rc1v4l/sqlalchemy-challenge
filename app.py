import numpy as np
import sqlalchemy
import datetime as dt
import numpy as np

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, distinct, desc
from flask import Flask, jsonify
from datetime import date


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    return (
        "<strong>Precipitation</strong><br> http://127.0.0.1:5000/api/v1.0/precipitation<br/>"
        "<strong>Stations</strong><br> http://127.0.0.1:5000/api/v1.0/stations<br/>"
        "<strong>Statistical temperature</strong><br> http://127.0.0.1:5000/api/v1.0/tobs<br/>"
        "<strong>Statistical temperature</strong><br> by starting date http://127.0.0.1:5000/api/v1.0/{start_date}<br/>"
        "<strong>Statistical temperature by range of dates</strong><br> http://127.0.0.1:5000/api/v1.0/{star_date}/{end_date}<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation_query():    
    session = Session(engine)
    
    today = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    today_datetime = date.fromisoformat(today[0])
    one_year_ago = today_datetime - dt.timedelta(days=365)
    
    precipitation_by_date_results = session.query(
        Measurement.date, Measurement.prcp
    ).filter(
        Measurement.date >= one_year_ago
    ).all()
    
    session.close()
    
    precipitation = list(np.ravel(precipitation_by_date_results))
    
    return jsonify(precipitation)

@app.route("/api/v1.0/stations")
def stations_query():
    session = Session(engine)
    
    stations_numbers = session.query(
        Measurement.station, func.count(Measurement.station).label("stations_count")
    ).group_by(
        Measurement.station
    ).order_by(
        desc("stations_count")
    )
    
    session.close()
    
    stations = list(np.ravel(stations_numbers))
    
    return jsonify(stations)

@app.route("/api/v1.0/tobs")
def statistical_temperature_query():    
    session = Session(engine)
    
    stations_numbers = session.query(
        Measurement.station,
        func.count(Measurement.station).label("stations_count")
    ).group_by(
        Measurement.station
    ).order_by(
        desc("stations_count")
    )

    first_one = True
    most_active_station = ""
    for row in stations_numbers:
        if first_one :
            most_active_station = row.station
            first_one = False
    
    today = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    today_datetime = date.fromisoformat(today[0])
    one_year_ago = today_datetime - dt.timedelta(days=365)

    station_temperature = session.query(
        func.max(Measurement.tobs).label("highest_temperature"),
        func.avg(Measurement.tobs).label("average_temperature"),
        func.min(Measurement.tobs).label("lowest_temperature")
    ).filter(
        Measurement.station == most_active_station
    )
    
    session.close()
    
    statistical_temperature = list(np.ravel(station_temperature))
    
    return jsonify(precipitation)

# @app.route("/api/v1.0/<start>")
# def statistical_temperature_query_by_initial_date(start):
#     """Fetch the Justice League character whose real_name matches
#        the path variable supplied by the user, or a 404 if not."""

#     canonicalized = real_name.replace(" ", "").lower()
#     for character in justice_league_members:
#         search_term = character["real_name"].replace(" ", "").lower()

#         if search_term == canonicalized:
#             return jsonify(character)

#     return jsonify({"error": f"Character with real_name {real_name} not found."}), 404


# @app.route("/api/v1.0/<start>/<end>")
# def statistical_temperature_query_by_range_of_dates(start, end):

#     for character in justice_league_members:
#         search_term = character["superhero"].replace(" ", "").lower()

#         if search_term == canonicalized:
#             return jsonify(character)

#     return jsonify({"error": "Character not found."}), 404

if __name__ == '__main__':
    app.run(debug=True)
