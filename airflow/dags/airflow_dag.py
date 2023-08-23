from datetime import datetime, timedelta
import os
import pytz
import csv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from airflow import DAG
from airflow.operators.python import PythonOperator

def update_csv():
    # Set up authentication
    scope = 'user-read-recently-played'  # Scope for accessing recently played tracks
    redirect_uri = 'http://localhost:3000'
    auth_manager = SpotifyOAuth(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, scope=scope, redirect_uri=redirect_uri)
    sp = spotipy.Spotify(auth_manager=auth_manager)

    # Define the CSV file path
    csv_file = 'user_playback_data.csv'

    # Retrieve the last updated track timestamp
    last_updated_track_timestamp = None
    if os.path.isfile('last_updated_track.txt'):
        with open('last_updated_track.txt', 'r') as f:
            last_updated_track_timestamp = f.read()

    # If the file doesn't exist or is empty, set a default timestamp
    if not last_updated_track_timestamp:
        last_updated_track_timestamp = '1970-01-01T00:00:00'  # or any other default value

    # Set your local timezone
    local_timezone = pytz.timezone('Asia/Kolkata')

    # Define the header row for the CSV file
    header = ['Track Name', 'Artist Name', 'Genres', 'Popularity', 'Duration (ms)', 'Explicit', 'Release Date', 'Time',
            'Danceability', 'Energy', 'Loudness', 'Speechiness', 'Instrumentalness', 'Valence', 'Tempo', 'Track Count']

    # If there is no last updated track timestamp, start from the current time
    if not last_updated_track_timestamp:
        last_updated_track_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    # Check if the CSV file exists
    csv_file_exists = os.path.isfile(csv_file)

    # Write the header row if the CSV file doesn't exist
    if not csv_file_exists:
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)

    # Retrieve recently played tracks since the last updated track
    recently_played_tracks = sp.current_user_recently_played(limit=50, after=last_updated_track_timestamp)['items']

    # Check if there are any recently played tracks since the last update
    if recently_played_tracks:
        # Update the last updated track timestamp to the most recent track in the batch
        last_updated_track_timestamp = recently_played_tracks[0]['played_at']

        # Iterate through the recently played tracks and update the dataset
        for track in recently_played_tracks:
            track_name = track['track']['name']
            artist_name = track['track']['artists'][0]['name']
            try:
                genres = track['track']['artists'][0]['genres']
            except KeyError:
                genres = ['Unknown']
            popularity = track['track']['popularity']
            duration_ms = track['track']['duration_ms']
            explicit = track['track']['explicit']
            release_date = track['track']['album']['release_date']
            played_at = track['played_at']
            played_at_datetime = datetime.strptime(played_at, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.utc)
            # Convert to local timezone
            played_at_datetime_local = played_at_datetime.astimezone(local_timezone)

            # Extract the date and time
            date = played_at_datetime_local.date()
            time = played_at_datetime_local.time()

            # Retrieve audio features
            audio_features = sp.audio_features(track['track']['id'])[0]
            danceability = audio_features['danceability']
            energy = audio_features['energy']
            loudness = audio_features['loudness']
            speechiness = audio_features['speechiness']
            instrumentalness = audio_features['instrumentalness']
            valence = audio_features['valence']
            tempo = audio_features['tempo']

            # Append the track data to the CSV file
            with open(csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([track_name, artist_name, genres, popularity, duration_ms, explicit, release_date, time,
                                danceability, energy, loudness, speechiness, instrumentalness, valence, tempo])

        # Update the last updated track timestamp in the file
        with open('last_updated_track.txt', 'w') as f:
            f.write(last_updated_track_timestamp)

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 7, 18),  # Replace with the desired start date and time
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'update_csv_dag',
    default_args=default_args,
    schedule=timedelta(hours=6)
)

# Set the task dependencies
update_csv_task = PythonOperator(
    task_id='update_csv_task',
    python_callable=update_csv,
    dag=dag
)
