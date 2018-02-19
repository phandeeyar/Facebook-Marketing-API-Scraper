import pandas as pd
import requests
import json
import csv
import datetime

places = pd.read_csv('facebookdata.csv',header=None)
access_token = ''

def url_constructor(place_id):
    ac_id = '&access_token='
    fields = '?fields=time_created,approximate_count,time_updated,targeting'
    base_url = 'https://graph.facebook.com/v2.11/'
    return base_url+place_id+fields+ac_id+access_token

def datahandler(data):
    approximate_count = data['approximate_count']
    target_id = data['id']
    age_max = data['targeting']['age_max']
    age_min = data['targeting']['age_min']
    time_created = datetime.datetime.strptime(data['time_created'],'%Y-%m-%dT%H:%M:%S+0000')
    time_created = time_created + datetime.timedelta(hours=-5) 
    time_created = time_created.strftime('%Y-%m-%d %H:%M:%S')     
    time_updated = datetime.datetime.strptime(data['time_updated'],'%Y-%m-%dT%H:%M:%S+0000')
    time_updated = time_updated + datetime.timedelta(hours=-5) 
    time_updated = time_updated.strftime('%Y-%m-%d %H:%M:%S')  

    if 'cities' in data['targeting']['geo_locations']:
        key = data['targeting']['geo_locations']['cities'][0]['key']
        name = data['targeting']['geo_locations']['cities'][0]['name']
        region = data['targeting']['geo_locations']['cities'][0]['region']
        region_id = data['targeting']['geo_locations']['cities'][0]['region_id']
    else:
        region = data['targeting']['geo_locations']['countries'][0]
        key=name=region_id= None
    return [target_id,key,name,region,region_id,approximate_count, age_max,age_min,time_created,time_updated]

def main():
    with open('./data/Marketing_data{}.csv'.format(datetime.datetime.now()), 'a') as file:
            w = csv.writer(file)
            w.writerow(["target_id","key","name","region","region_id","approximate_count", "age_max","age_min","time_created","time_updated","ad_id"])
            for place in places[0]:
                response = requests.get(url_constructor(str(place)))
                print(response.text)
                js = datahandler(json.loads(response.text))
                w.writerow(js)

if __name__ == '__main__':
    main()
