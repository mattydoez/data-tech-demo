import requests
from datetime import datetime, timedelta
import os
import pytz 

def get_weather(zip_code, date):

    # Define the Pacific Daylight Time (PDT) timezone
    pdt_timezone = pytz.timezone('America/Los_Angeles')

    # Convert the given date to the PDT timezone
    date_pdt = date.astimezone(pdt_timezone)

    # Convert PDT date to Unix timestamp
    unix_timestamp = int(date_pdt.timestamp())
    print(unix_timestamp)

    # OpenWeatherMap API endpoint
    api_url = "https://api.openweathermap.org/data/2.5/forecast"
    
    # API key (replace 'your_api_key' with your actual API key)
    api_key = os.environ['WEATHER_API_KEY']
    
    # Query parameters
    params = {
        "zip": zip_code,
        "appid": api_key,
        "units": "imperial",  # Use metric for Celsius
        "dt": unix_timestamp   # Date for future weather
    }
    
    # Make API request
    response = requests.get(api_url, params=params)
    
    # Check if request was successful
    if response.status_code == 200:
        data = response.json()
        # Extract weather information
        weather_info = data['list'][0]  # First entry for the specified date
        weather_description = weather_info['weather'][0]['description']
        temperature = weather_info['main']['temp']
        humidity = weather_info['main']['humidity']
        wind_speed = weather_info['wind']['speed']
        # Print weather details
        print(f"Weather forecast for {zip_code} on {date.strftime('%Y-%m-%d')}:")
        print(f"Description: {weather_description}")
        print(f"Temperature: {temperature} °F")
        print(f"Humidity: {humidity}%")
        print(f"Wind Speed: {wind_speed} mph")
        return data
    else:
        print("Failed to fetch weather data. Please check your API key or try again later.")

def print_json_fields(json_obj, parent_key=''):
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            new_key = parent_key + '.' + key if parent_key else key
            if isinstance(value, dict) or isinstance(value, list):
                print_json_fields(value, new_key)
            else:
                print(new_key)
    elif isinstance(json_obj, list):
        for item in json_obj:
            print_json_fields(item, parent_key)

# Example usage
if __name__ == "__main__":
    zip_code = "91326"  # Replace with desired zip code
    target_date = datetime.now() 
    get_weather(zip_code, target_date)
    print_json_fields(get_weather(zip_code, target_date))


# TODO: Add way to include future dates for up to a month and 
# only pull in key indictors