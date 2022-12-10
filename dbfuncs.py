
import psycopg2

def getDBConnection (config, databasename) :
    dbConn = psycopg2.connect(user = config['postgreSQL']['username'],
        password = config['postgreSQL']['password'],
        host = config['postgreSQL']['hostname'],
        port = config['postgreSQL']['port'],
        database = databasename)
    return dbConn

def getPostgresString(config):
    if (config['postgreSQL']['username'] == ""):
        postgresStr = 'postgresql://' + config['postgreSQL']['hostname']  + '/' + config['postgreSQL']['database']
    elif (config['postgreSQL']['password'] == "") :
       postgresStr = 'postgresql://' + config['postgreSQL']['username'] + '@' + config['postgreSQL']['hostname']  + '/' + config['postgreSQL']['database']
    else :
       postgresStr = 'postgresql://' + config['postgreSQL']['username'] + ':' + config['postgreSQL']['password'] + '@' + config['postgreSQL']['hostname']  + '/' + config['postgreSQL']['database']

    return postgresStr
    
