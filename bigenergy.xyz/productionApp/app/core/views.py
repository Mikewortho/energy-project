from flask import Blueprint, request, render_template, flash, g, session, redirect, url_for, \
    abort, jsonify, Flask
from app.core.repository import *
import os

mod = Blueprint('core', __name__)
app = Flask(__name__)
app._static_folder = '/home/bigenergy/bigenergy.xyz/testapp/app/static'



@mod.route('/')
def index():
    repository = Repository()
    return (render_template('core/home.html'))

@app.route('/receiver', methods= ['POST'])
def worker():
    # Read data sent to it and reply
    data = request.get_json()
    csv_string = "../../gen_data/" + parseParameters['BA'] + "_hourly_" + parseParameters['timePrecision'] + ".csv"

    return csv_string

@mod.route('/gen_data/state')
def returnStateColour():
    #return send_file('/home/bigenergy/bigenergy.xyz/static/gen_data/stateColours.csv')
    return send_file('/static/data/stateColours.csv')

    #return os.getcwd()

@mod.route('/beverley')
def beverley():
    return "Hi dad!"
