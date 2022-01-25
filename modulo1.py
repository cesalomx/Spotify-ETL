import psycopg2
import sqlalchemy as sa
from sqlalchemy import create_engine
    
def loading():
    
    #psycopg2 is only used when connecting to a PostgreSQL Database, so we first make contact by setting some basic info.
    conn = psycopg2.connect(host='127.0.0.1',port='5432',dbname='Athenas',user='postgres',password='cis15a')
    #Creating a cursor to display my PostgreSQL Version
    cur = conn.cursor()
    print('=============================================================')
    print('Connected to Athenas')
    print('PostgreSQL database version:')
    print("=============================================================")
    cur.execute('SELECT version()')
    db_version = cur.fetchone()
    print(db_version)

    #Creating the "Spotify_API" table in PostgreSQL using the psycopg2 library.
    table_py = '''CREATE TABLE IF NOT EXISTS spotify(
        unique_identifier SERIAL PRIMARY KEY,
        artist_id VARCHAR(255) NOT NULL,
        artist_link VARCHAR(255) NOT NULL,
        album_id VARCHAR(255) NOT NULL,
        album_name VARCHAR(255) NULL,
        album_link VARCHAR(255) NOT NULL,
        song_id VARCHAR(255) NOT NULL,
        song_name VARCHAR(255) NOT NULL,
        song_link VARCHAR(255) NOT NULL,
        duration_ms INT NOT NULL,
        popularity INT NULL,
        disc_number INT NULL,
        played_at TIMESTAMP NOT NULL,
        date DATE NOT NULL,
        time TIME NOT NULL
        )'''
    #Executing my "table" using my cur variable.
    cur.execute(table_py)
    print("=============================================================")
    print("=============================================================")
    print("=============================================================")
    print("=============================================================")
    print("=============================================================")
    print('Table created succesfully')
    print("=============================================================")
    print("=============================================================")
    print("=============================================================")
    print("=============================================================")
    print("=============================================================")
    #In order to load my existing dataframe to the table we previously created using the psycopg2 library, 
    #we now need to create an engine using SQLALCHEMY and APPEND my dataframe to the spotify_API Table.        
    engine = sa.create_engine('postgresql://postgres:cis15a@localhost:5432/Athenas')
    df.to_sql('spotify', con = engine, index=False, if_exists='append')
    print("=============================================================")
    print('The ETL ran succesfully')
    print("=============================================================")
    cur.close()
    conn.commit()
    

# if __name__ == "__main__":
#     print("ejecutando como programa principal")
#     loading()
    