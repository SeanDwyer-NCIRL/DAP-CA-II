
import psycopg2

def getDBConnection (config, databasename) :
    dbConn = psycopg2.connect(user = config['postgreSQL']['username'],
        password = config['postgreSQL']['password'],
        host = config['postgreSQL']['hostname'],
        port = config['postgreSQL']['port'],
        database = databasename)
    return dbConn     