# Import the dependencies.
import numpy as np
import re
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect
from sqlalchemy.sql import exists 

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

print(Base.classes.keys())

# Save reference to the table
measurement = Base.classes.measurement
station = Base.classes.station
# Get the column information for the Station table
inspector = inspect(engine)
columns_station = inspector.get_columns('station')
columns_measurement = inspector.get_columns('measurement')

# column names in the station table
for column in columns_station:
    print(column['name'], column['type'])
# column names in the station table
for column in columns_measurement:
    print(column['name'], column['type'])
#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return(
        f"Available Routes:<br/>"
        f'Look at the precipitaion data for the past year: /api/v1.0/precipitation <br/>'
        f'Look at a list of the stations: /api/v1.0/stations <br/>'
        f'Look at the temperature for the past year: /api/v1.0/tobs <br/>'
        f'Look at the temperatures for the past year for most active station: /api/v1.0/mstacttemp <br/> '
        f'To find the min, max, and avg temperature from a certain date: /api/v1.0/start <br/>'
        f'To find the min, max, and avg temperature between specific dates: /api/v1.0/start/end <br/>'
    )

@app.route('/api/v1.0/precipitation')
def precipitation():
    """Jsonify precipitation data for one year."""
    session = Session(engine)
    #Find the most recent date
    last_date = session.query(func.max(measurement.date)).scalar()
    # Calculate the date one year from the last date in data set.
    date_one_yr_ago_dt = dt.datetime.strptime(last_date, '%Y-%m-%d') - dt.timedelta(days=365)
    query_date = date_one_yr_ago_dt.strftime('%Y-%m-%d')
    # Perform a query to retrieve the date and precipitation scores
    last_year_prcp = session.query(measurement.date, measurement.prcp).\
            filter(measurement.date >= query_date).all()
    results = []
    for date, prcp in last_year_prcp:
        result_dict = {}
        result_dict["date"] = date
        result_dict["prcp"] = prcp
        results.append(result_dict)
    # Close Session
    session.close()
    # Return the results as a JSON dictionary
    return jsonify(results)

@app.route('/api/v1.0/stations')
def get_stations():
    # Create session (link) from Python to the DB
    session = Session(engine)

    # Query Stations
    results = session.query(station.station).all()

    # Convert list of tuples into normal list
    station_details = list(np.ravel(results))

    return jsonify(station_details)

@app.route('/api/v1.0/tobs')
def tobs():
    """Return the temperatures from the past year. """
    session = Session(engine)

    #Find the most recent date
    last_date = session.query(func.max(measurement.date)).scalar()
    # Calculate the date one year from the last date in data set.
    date_one_yr_ago_dt = dt.datetime.strptime(last_date, '%Y-%m-%d') - dt.timedelta(days=365)
    query_date = date_one_yr_ago_dt.strftime('%Y-%m-%d')

 #Set up query to get temperature

    results = session.query(measurement.date, measurement.tobs).\
        filter(measurement.date >= query_date)
    
    session.close()

    temp_results = []

    for result in results:
        line = {}
        line["Date"] = result[0]
        line["Temperature"] = int(result[1])
        temp_results.append(line)
    return jsonify(temp_results)

@app.route('/api/v1.0/mstacttemp')
def mstacttemp():
    """Return the temperatures from the past year of the most active station. """
    session = Session(engine)

    #Find the most recent date
    last_date = session.query(func.max(measurement.date)).scalar()
    # Calculate the date one year from the last date in data set.
    date_one_yr_ago_dt = dt.datetime.strptime(last_date, '%Y-%m-%d') - dt.timedelta(days=365)
    query_date = date_one_yr_ago_dt.strftime('%Y-%m-%d')

    # Most active station 
    active_stations = session.query(measurement.station,func.count(measurement.id)).group_by(measurement.station).order_by(func.count(measurement.id).desc()).first()
    most_active = active_stations[0]


 #Set up query to get temperature

    query_result = session.query(measurement.station,measurement.date, measurement.tobs).\
        filter((measurement.date >= query_date),(measurement.station == most_active)).all()
    
    session.close()

    temp_results = []

    for result in query_result:
        line = {}
        line['Station'] = result[0]
        line["Date"] = result[1]
        line["Temperature"] = int(result[2])
        temp_results.append(line)
    return jsonify(temp_results)

@app.route('/api/v1.0/<start>')
def start(start):
    """Jsonify temperature data from a singular date."""

    # Create session (link) from Python to the DB
    session = Session(engine)

    # Date Range (only for help to user in case date gets entered wrong)
    date_range_max = session.query(measurement.date).order_by(measurement.date.desc()).first()
    date_range_max_str = str(date_range_max)
    date_range_max_str = re.sub("'|,", "",date_range_max_str)
    print (date_range_max_str)

    date_range_min = session.query(measurement.date).first()
    date_range_min_str = str(date_range_min)
    date_range_min_str = re.sub("'|,", "",date_range_min_str)
    print (date_range_min_str)


    # Check for valid entry of start date
    valid_entry = session.query(exists().where(measurement.date == start)).scalar()
 
    if valid_entry:
        results = (session.query(func.min(measurement.tobs),func.avg(measurement.tobs),func.max(measurement.tobs)).filter(measurement.date >= start).all())
        tmin =results[0][0]
        tavg ='{0:.4}'.format(results[0][1])
        tmax =results[0][2]
        result_printout =(['Entered Start Date: ' + start, 'The lowest Temperature was: '  + str(tmin) + ' F','The average Temperature was: ' + str(tavg) + ' F','The highest Temperature was: ' + str(tmax) + ' F'])
        return jsonify(result_printout)

    return jsonify({"error": f"Input Date {start} not valid. Date Range is {date_range_min_str} to {date_range_max_str}"}), 404
   

@app.route("/api/v1.0/<start>/<end>") # Calculate the `TMIN`, `TAVG`, and `TMAX` for dates between the start and end date inclusive
def start_end(start, end):

    # Create session (link) from Python to the DB
    session = Session(engine)

    # Date Range (only for help to user in case date gets entered wrong)
    date_range_max = session.query(measurement.date).order_by(measurement.date.desc()).first()
    date_range_max_str = str(date_range_max)
    date_range_max_str = re.sub("'|,", "",date_range_max_str)
    print (date_range_max_str)

    date_range_min = session.query(measurement.date).first()
    date_range_min_str = str(date_range_min)
    date_range_min_str = re.sub("'|,", "",date_range_min_str)
    print (date_range_min_str)

    # Check for valid entry of start date
    valid_entry_start = session.query(exists().where(measurement.date == start)).scalar()
 	
 	# Check for valid entry of end date
    valid_entry_end = session.query(exists().where(measurement.date == end)).scalar()

    if valid_entry_start and valid_entry_end:
        results = (session.query(func.min(measurement.tobs),func.avg(measurement.tobs),func.max(measurement.tobs)).filter(measurement.date >= start).filter(measurement.date <= end).all())
        tmin =results[0][0]
        tavg ='{0:.4}'.format(results[0][1])
        tmax =results[0][2]
        result_printout =( ['Entered Start Date: ' + start,'Entered End Date: ' + end,'The lowest Temperature was: '  + str(tmin) + ' F','The average Temperature was: ' + str(tavg) + ' F','The highest Temperature was: ' + str(tmax) + ' F'])
        return jsonify(result_printout)

    if not valid_entry_start and not valid_entry_end:
    	return jsonify({"error": f"Input Start {start} and End Date {end} not valid. Date Range is {date_range_min_str} to {date_range_max_str}"}), 404
    
if __name__ == '__main__':
    app.run(debug=True)