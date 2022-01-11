# Spotify-ETL

The [main_.py](https://github.com/cesalomx/Spotify-ETL/blob/main/main_.py) file uses [this](https://api.spotify.com/v1/me/player/recently-played) *endpoint* to extract the *most Recent Played Tracks* out of the spotify API. After performing the extraction, I performed a basic clean-up of the data extracted as well as creating a unique identifier for the load-up of my dataframe to a PostgreSQL database, for which I used SQL.

## Extraction

The data was extracted using the spotify API mentioned up above to get the most recent 20 played tracks in spotify by sending a request to the API. The result of this is a .json response stored in the *response* variable, this dictionary was used to extract specific values out of our response to create a dictionary with all of our data and then, appending it to a list to be later converted to a DataFrame using pandas.


## Transforming
The transformation of my dataframe consisted of some basic checks here and there, starting by converting my list to a dataframe, re-ordering the dataframe columns and changing the dataype of *datetime*, *date* & *time*.


## Loading to PostgreSQL
Now that we are done with our basic checks using pandas, we use the [psycopg2](https://pypi.org/project/psycopg2/) library to create a connection to an existing database in postgreSQL, starting by creating a table with a *unique_identifier* along with our key values according to what we've got in our DataFrame. Last but no least, in order to load the Dataframe to our table called *spotify*, we have to create an engine using the *sqlalchemy* libray to append my existing dataframe to such table.


![database_output_image](https://user-images.githubusercontent.com/63975528/148864777-53c10231-4d37-4425-bd96-7e968b8385fd.jpg)
