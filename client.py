#!/usr/bin/python3

import click
import xmlrpc.client

global proxy


@click.command()
@click.option('--createworker', 'createworker_flag', default=False, flag_value='createworker', help='Create a new worker')
@click.option('--deleteworker', default=-1, help='Delete a worker by ID')
@click.option('--listworkers', 'listworkers_flag', default=False, flag_value='listworkers', help='List all workers')
@click.option('--countingwords', default="", help='Call CountingWords for the specified files (files separated by commas)')
@click.option('--wordcount', default="", help='Call WordCount for the specified files (files separated by commas)')



def input(createworker_flag, deleteworker, listworkers_flag, countingwords, wordcount):
    global proxy
    if createworker_flag:
        print('Creating worker with ID: ' + str(proxy.create_worker()))
    if deleteworker != -1:
        proxy.delete_worker(deleteworker)
    if listworkers_flag:
        print(str(proxy.list_workers()))
    if countingwords != "":
        print(proxy.put_task('counting_words', countingwords))
    if wordcount != "":
	    print(proxy.put_task('word_count', wordcount))


if __name__ == '__main__':
    proxy = xmlrpc.client.ServerProxy('http://localhost:9000')

    input()

