from sqlalchemy import create_engine
import pandas as pd
import requests
import sqlalchemy as sa
import psycopg2
import modulo1

if __name__ == "__main__":
    
    def extraction():
        
        TOKEN = 'BQBO6rHoauhrh3jhJ1-K-P9fil25Vu7pHvbMoVQFG_j0vxn4glCxLPbQ_1WHIR_WNnS72naTFfJWWpjDWwn1zmZIAUHMkluUL945WakcdbAOr-3xndX9vpkCUdCfGYmsUfomTKrcOIYWwD9U9PS8EiTpaF3HcT6gePiQh-fW'

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
    
    extraction()
    
    modulo1.loading()