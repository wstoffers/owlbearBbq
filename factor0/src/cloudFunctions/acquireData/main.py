#**    This line is 79 characters long.  The 80th character should wrap.   ***\

#imports:
import json
import pyowm
import pytz
from datetime import datetime
from google.cloud import secretmanager
from google.cloud import storage

#define:
def acquireData(request):
    """Entry point for HTTP Cloud Function
    Args:
        request (flask.Request): The request object

    Returns:
        'Success'

    """
    
    co = datetime.now().astimezone(pytz.timezone("America/Denver"))
    coTime = f'{co:%Y-%m-%d-%H.%M.%S.%f-%a}'
    owm = pyowm.OWM(owmApiKey())
    manager = owm.weather_manager()
    owlbear = manager.weather_at_coords(39.7605, -104.9823).weather.to_dict()
    franklin = manager.weather_at_coords(30.2703, -97.7311).weather.to_dict()
    writeToGcs([owlbear,franklin],
               [f'owlbear{coTime}.json',f'franklin{coTime}.json'])
    return 'Success'

def writeToGcs(weathers, names):
    """Writes weather data as JSON to raw bucket
    Args:
        weathers: Iterable of OpenWeatherMap weather dicts (no relation to Carl)
        names: Iterable of strings representing JSON file names

    """
    
    storageClient = storage.Client()
    rawBucket = 'wstoffers-galvanize-owlbear-data-lake-raw'
    bucket = storageClient.get_bucket(rawBucket)
    for weather, name in zip(weathers, names):
        blob = bucket.blob(name)
        blob.upload_from_string(data=json.dumps(weather),
                                content_type='application/json')

def owmApiKey():
    """Retrieves OpenWeatherMap API key from Google Cloud Secrets
    Args:
        None

    Returns:
        payload: API key as a string

    """
    
    secretClient = secretmanager.SecretManagerServiceClient()
    version = "versions/latest"
    name = f"projects/owlbear-bbq/secrets/openWeatherSecret/{version}"
    response = secretClient.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8").strip()
    return payload

#run:
if __name__ == '__main__':
    #creatively test cloud function code, overwriting functions/methods
    pass
