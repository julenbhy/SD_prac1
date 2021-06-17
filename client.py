#!/usr/bin/python3

import click
import xmlrpc.client
global proxy

# Definicion parametros entrada y funcion cli() para gestionar las diferentes RPC sobre el master
@click.command()
@click.option('--createw', 'createw_flag', default=False, flag_value='wcreate', help='Create a new worker')
@click.option('--deletew', default=-1, help='Delete a worker by ID')
@click.option('--listw', 'listw_flag', default=False, flag_value='wlist', help='List all workers')
@click.option('--countingwords', default="", help='Call CountingWords for the specified files (files separated by commas)')
@click.option('--wordcount', default="", help='Call WordCount for the specified files (files separated by commas)')



def input(createw_flag, deletew, listw_flag, countingwords, wordcount):
    global proxy
    if createw_flag:
        print('Creating worker with ID: ' + str(proxy.create_worker()))
    if deletew != -1:
        proxy.delete_worker(deletew)
    if listw_flag:
        print(str(proxy.list_workers()))
    if countingwords != "":
        print(proxy.put_task('counting_words', countingwords))
    if wordcount != "":
	    print(proxy.put_task('word_count', wordcount))


# El main solo iniciara la conexion xmlrpc y llamara a la funcion que gestionara los parametros
if __name__ == '__main__':
    proxy = xmlrpc.client.ServerProxy('http://localhost:9000')

    input()

