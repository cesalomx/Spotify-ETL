from sqlalchemy import create_engine
import pandas as pd
import requests
import sqlalchemy as sa
import psycopg2

if __name__ == "__main__":
    
    def extraction():
        
        TOKEN = 'BQAsHgoVPdj_C4rc4Tue4BEk2Gfsn94bkgAHESWyzUfKKN-IpK48I9YyNp-89fSWT9gSsv4HNIxTiWBI74GT3vu-eH2NcIS6XmgSEJeMd4PPNzCv402W_oNHvB3ert2mkcT6YV8J-V7bR3glb971fuvBtB6FCLJjQS4Jtyby'

        #We need headers to send the information along with our request, so this should be part of our request.
        headers = {
            "Accept":"application/json",
            "Content-Type":"application/json",
            "Authorization":"Bearer {token}".format(token=TOKEN)
        }

        r = requests.get("https://api.spotify.com/v1/me/player/recently-played",headers = headers)
        response = r.json()

        if 'error' in response:
            print('The TOKEN is either wrong or has expired')
        else:
        #if my response went smoothly, then we proceed to extract and loop through my .json dictionary and get the values from it.
            my_song_list = []
            global df

            for song in response['items']:
                artist_id = song['track']['artists'][0]['id']
                artist_name = song['track']['artists'][0]['name']
                artist_link = song['track']['artists'][0]['external_urls']['spotify'] 
                album_id = song['track']['album']['id']
                album_name = song['track']['album']['name']
                album_link = song['track']['album']['external_urls']['spotify']
                song_id = song['track']['id']
                song_name = song['track']['name']
                song_link = song['track']['external_urls']['spotify']
                duration_ms = song['track']['duration_ms']
                popularity = song['track']['popularity']
                disc_number = song['track']['disc_number']
                played_at = song['played_at'].split(".")[0]
                
                song_dic = {'artist_id': artist_id,
                                'artist_name':artist_name,
                                'artist_link':artist_link,
                                'album_id':album_id,
                                'album_name':album_name,
                                'album_link':album_link,
                                'song_id':song_id,
                                'song_name':song_name,
                                'song_link':song_link,
                                'duration_ms':duration_ms,
                                'popularity':popularity,
                                'disc_number':disc_number,
                                'played_at':played_at
                                }
                
                my_song_list.append(song_dic) #now, in order to convert my DICTIONARY to a DATAFRAME, I should consider appending it to a LIST first.
                
                df = pd.DataFrame(my_song_list) #now that all my songs are in a LIST datatype, I can convert it to a dataframe.
                
        #This is a basic transformation performed in my dataframe:
            
        #Re-ordering columns in my df
        df = df[["artist_id","artist_link","album_id","album_name","album_link","song_id","song_name","song_link","duration_ms","popularity","disc_number","played_at"]]
        #Creating two columns (date, time) by spliting the played_at column.
        df[['date','time']] = df['played_at'].str.split('T',expand=True)
        # #Right now, played_at, date & time are objects, so we need to change these to timestamp.
        df['date'] = pd.to_datetime(df['date'])
        df['time'] = pd.to_datetime(df['time'])
        df['played_at'] = pd.to_datetime(df['played_at'])
        df['played_at'] = df['played_at'].dt.tz_localize('US/Central')
        
        return df

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
        if cur.execute(table_py) == True:
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
        else:
            print("=============================================================")
            print("=============================================================")
            print("=============================================================")
            print("=============================================================")
            print("Something came up with your database")
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
        print("=============================================================")
        print('The ETL ran succesfully')
        print("=============================================================")
        print("=============================================================")
        cur.close()
        conn.commit()

    extraction()
    loading()
