from flask import Flask, jsonify

from flask_restful import Resource, Api

from sqlalchemy import create_engine

import multiprocessing

import urllib

import threading

import logging

from waitress import serve

import datetime

from insert_wts import WTSInsert

def WTSJob(batch_id, params):

wts_job= WTSInsert(batch_id, params)

jobProc = multiprocessing. Process (target wts_job.main, args=())

jobProc.start()

jobProc.join()

logging.info('WTS Job completed')

return

class JobController (Resource):

def get(self, job_id):

if job_id == 'wtsinsert':

batch_id=CreateBatchEntry('WTS Insert')

if batch_id != False:

threading.Thread(target=WTSJob, args=(batch_id, params)).start() logging.info('creating batch entry and starting WTS Insert')

response_dict={'title': 'wts Insert response trigger', 'status': 200, 'details': 'Batch run log id is + str(batch_id),

'timestamp':str(datetime.datetime.now()), 'message': '', 'batchId': batch_id}

return jsonify (response_dict)

else:

return 'Error: Job has been called within the last minute.'￼Enter
