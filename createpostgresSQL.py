import dbfuncs as db
import yaml
import psycopg2


def createDBIfNotExists (config):
    try:
        dbConnection = db.getDBConnection(config, 'postgres')
        with dbConnection.cursor() as cursor:
                cursor.execute('SELECT FROM pg_database WHERE datname = \'' +  config['postgreSQL']['database'] + '\'')
                data = cursor.fetchall()
               
    except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
    finally:
        cursor.close()
        if(dbConnection):
            dbConnection.close()

    if (not data):
        try:
            print("Creating database " + config['postgreSQL']['database'])
            conn = db.getDBConnection(config,'postgres')
            conn.autocommit = True
            cursor = conn.cursor()
            #sql = '''CREATE DATABASE cospdb'''
            sql = '''CREATE DATABASE ''' + config['postgreSQL']['database']
            cursor.execute(sql)
        except (Exception , psycopg2.Error) as dbError :
            print ("Error:", dbError)
        finally:
            cursor.close()
            if(dbConnection):
                dbConnection.close() 


## Read DB Configs
config = yaml.safe_load(open("config.yaml"))

# 
print ("Creating PostgresSQL database cospdb")
createDBIfNotExists(config)

