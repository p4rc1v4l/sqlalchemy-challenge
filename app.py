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
    "<strong>Temperature observations of the most active station for the last year</strong><br> http://127.0.0.1:5000/api/v1.0/tobs<br/>"
    "<strong>Temperature observations by starting date (year-month-day)</strong><br> http://127.0.0.1:5000/api/v1.0/{date}<br/>"
    "<strong>Temperature observations by range of dates (year-month-day)</strong><br> http://127.0.0.1:5000/api/v1.0/{star_date}/{end_date}<br/>"
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
    
    list = []
    for row in precipitation_by_date_results:
        list.append({row.date: row.prcp})
    
    return jsonify(list)

@app.route("/api/v1.0/stations")
def stations_query():
    session = Session(engine)
    
    results = session.query(
        Station.id, Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation
    ).all()
    
    session.close()
    
    stations = list(np.ravel(results))
    
    return jsonify(results)

@app.route("/api/v1.0/tobs")
def statistical_temperature_query():    
    session = Session(engine)
    
    today = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    today_datetime = date.fromisoformat(today[0])
    one_year_ago = today_datetime - dt.timedelta(days=365)
    
    stations_numbers = session.query(
        Measurement.station,
        func.count(Measurement.station).label("stations_count")
    ).group_by(
        Measurement.station
    ).order_by(
        desc("stations_count")
    ).all()

    first_one = True
    most_active_station = ""
    for row in stations_numbers:
        if first_one :
            most_active_station = row.station
            first_one = False
    
    temperature_observations = session.query(
        Measurement.tobs,
        func.count(Measurement.tobs).label("observations")
    ).filter(
        Measurement.date >= one_year_ago
    ).filter(
        Measurement.station == most_active_station
    ).group_by(
        Measurement.tobs
    ).order_by(
        desc("observations")
    ).all()

    session.close()
        
    return jsonify(temperature_observations)

@app.route("/api/v1.0/<date>")
def statistical_temperature_query_by_initial_date(date):
    date_parts = date.split('-')
    start_date = dt.date(int(date_parts[0]), int(date_parts[1]), int(date_parts[2]))
    
    session = Session(engine)
    
    station_temperature = session.query(
        func.max(Measurement.tobs).label("highest_temperature"),
        func.avg(Measurement.tobs).label("average_temperature"),
        func.min(Measurement.tobs).label("lowest_temperature")
    ).filter(
        Measurement.date >= start_date
    )

    session.close()

    results = station_temperature.one()
    highest_temperature = results.highest_temperature
    lowest_temperature = results.lowest_temperature
    average_temperature = results.average_temperature
    
    list = []
    list.append({"Highest temperature" : highest_temperature})
    list.append({"Lowest temperature" : lowest_temperature})
    list.append({"Average temperature" : average_temperature})
    
    return jsonify(list)

@app.route("/api/v1.0/<star_date>/<end_date>")
def statistical_temperature_query_by_dates_range(star_date, end_date):
    start_date_parts = star_date.split('-')
    end_date_parts = end_date.split('-')
    start_date_date = dt.date(int(start_date_parts[0]), int(start_date_parts[1]), int(start_date_parts[2]))
    end_date_date = dt.date(int(end_date_parts[0]), int(end_date_parts[1]), int(end_date_parts[2]))
    
    session = Session(engine)
    
    station_temperature = session.query(
        func.max(Measurement.tobs).label("highest_temperature"),
        func.avg(Measurement.tobs).label("average_temperature"),
        func.min(Measurement.tobs).label("lowest_temperature")
    ).filter(
        Measurement.date >= start_date_date
    ).filter(
        Measurement.date <= end_date_date
    )

    session.close()

    results = station_temperature.one()
    highest_temperature = results.highest_temperature
    lowest_temperature = results.lowest_temperature
    average_temperature = results.average_temperature
    
    list = []
    list.append({"Highest temperature" : highest_temperature})
    list.append({"Lowest temperature" : lowest_temperature})
    list.append({"Average temperature" : average_temperature})
    
    return jsonify(list)

if __name__ == '__main__':
    app.run(debug=True)
