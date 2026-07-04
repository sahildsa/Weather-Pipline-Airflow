import requests
import csv

url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
def fetch_data(url)->dict:
    """
    Checks the status code of the response from the given URL and returns the JSON data if the request is successful.
    
    args:
        url (str): The URL to send the GET request to.
    returns:
        dict: The JSON data from the response if the request is successful.
    """
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")
    
def save_data_to_csv(data: dict)->str:
    """
    Saves the given data to a CSV file at the specified file path.
    
    args:
        data (dict): The data to be saved to the CSV file.
        file_path (str): The path to the output CSV file.
    """
    import os 
    import datetime
    #cwd=os.path.dirname(os.path.realpath(__file__)) #CWD =current working directory
    cwd=os.getcwd() #CWD =current working directory
    cwdreport=os.path.join(cwd, "result") #results folder in the current working directory
    file_path = os.path.join(cwdreport, f"sample_data_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")
    
    current_weather = data.get("current")
    if not current_weather:
        print("No current weather data to save.")
        return None

    fieldnames = current_weather.keys()
    with open(file_path, mode='w', newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        # writer.writeheader() # This would write the header, it's commented out as you don't want it.
        writer.writerow(current_weather)
    return file_path
    

if __name__ == "__main__":
    data = fetch_data(url)
    if data:
        output_file = save_data_to_csv(data)
        if output_file:
            print(f"Data saved to {output_file}")