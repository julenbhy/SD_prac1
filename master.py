#!/usr/bin/env python3
 
from collections import Counter
from multiprocessing import Process

import redis
import requests

from  xmlrpc.server import SimpleXMLRPCServer
import logging

import json


global r

WORKERS = {}
WORKER_ID = 0
DELIMITER = ','


#############################################
####			WORKERS' CODE 			#####
#############################################


def get_redis_petition(key):
	global r
	return r.blpop(key)[1].decode()


def get_http_data(filename):
	return requests.get("http://localhost:8000/"+filename).content.decode()



def word_count(filename):
	data =  get_http_data(filename)
	words = data.split()
	words = list(dict.fromkeys(words))
	return len(words)


def counting_words(filename):
	data =  get_http_data(filename)
	wordcount = dict(Counter(data.split()))
	return json.dumps(wordcount)


def start_worker(proc_id):
	global r
	while True:
		task = get_redis_petition("petitions").split(DELIMITER)

		if task[0]=="word_count":
			#print(word_count(task[1]))# + " task dispatched by worker " + str(proc_id))
			r.rpush("results", str(word_count(task[1])))

		elif task[0]=="counting_words": 
			#print(counting_words(task[1]))# + " task dispatched by worker " + str(proc_id))
			r.rpush("results", str(counting_words(task[1])))

		elif task[0]=="join":

			if task[1]=="word_count":
				result = 0
				num = int(task[2])
				while(num>0):
					result += int(get_redis_petition("results"))
					num -= 1
				r.rpush("answer", str(result))

			elif task[1]=="counting_words":
				final_dict={}
				num = int(task[2])
				while(num>0):
					a = json.loads(get_redis_petition("results"))
					#join the new dictionary
					final_dict = {x: final_dict.get(x, 0) + a.get(x, 0)
                    for x in set(final_dict).union(a)}
					num -= 1
				r.rpush("answer", str(final_dict))




#############################################
####			MASTER'S CODE			#####
#############################################

def create_worker():
	global WORKERS
	global WORKER_ID
	proc = Process(target=start_worker, args=(WORKER_ID, ))
	proc.start()
	WORKERS[WORKER_ID] = proc
	WORKER_ID += 1
	return WORKER_ID-1


def delete_worker(id):
	global WORKERS
	WORKERS[id].terminate()
	del WORKERS[id]



def list_workers():
	global WORKERS
	return str(WORKERS)


def put_task(task, filenames):
	logging.info('put_task(%s %s)', task, filenames)

	global r
	pet_length=0

	#split the filenames
	filenames = filenames.split(DELIMITER)

	#submit the tasks to redis petition queu
	for filename in filenames:
		r.rpush("petitions", task+DELIMITER+filename)
		pet_length += 1
	r.rpush("petitions", "join"+DELIMITER+str(task)+DELIMITER+str(pet_length))

	#get the result of the petition from redis
	answer = get_redis_petition("answer")
	return answer




#############################################
#####				MAIN				#####
#############################################

if __name__ == '__main__':

	r = redis.Redis()
	r.flushall()

	#set up loggin
	logging.basicConfig(level=logging.INFO)

	server = SimpleXMLRPCServer(
			("localhost", 9000), 
			logRequests=True,
			allow_none=True
	)
	server.register_function(create_worker)
	server.register_function(delete_worker)
	server.register_function(list_workers)
	server.register_function(put_task)


	try:
		print("use Control -C to exit")
		server.serve_forever()
	except KeyboardInterrupt:
		print("Exiting")