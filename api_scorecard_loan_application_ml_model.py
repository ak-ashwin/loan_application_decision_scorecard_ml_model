import datetime
import logging
import os

from flask import Flask, request
from flask_cors import CORS

from service_api_scorecard.service_api_scorecard import cibil_flat_data_func

application = Flask(__name__)


@application.route('/', methods=['GET'])
def hello():
    return "hello"


@application.route('/scorecard_loan_decision_ml_model/v1/business_id/<business_id>', methods=['GET'])
def loan_decision_ml_model(business_id):
    return cibil_flat_data_func(business_id)

CORS(application)

if __name__ == "__main__":
    application.run(host='0.0.0.0')
