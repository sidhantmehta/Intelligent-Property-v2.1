import pandas as pd
import json as json
import requests
import csv


class HereMapping:
    '''abcd@plutocow.com / 0nlinePassw0rd'''
    app_id = 'lQpVCrqm0th14AbGqBrk'
    app_code = 'ahWJaKNZ7JMY2yzjKE0LvQ'
    categories = ['public-transport', 'airport', 'eat-drink', 'railway-station', 'recreation']

    # Get Postcode long/lat and define categories

    starting_position = 0

    def __init__(self, postcode_outcodes_filepath, base_folder_for_output):
        if postcode_outcodes_filepath !='': self.outp = csv.reader(open(postcode_outcodes_filepath, 'rb'), delimiter=",")
        self.base_folder = base_folder_for_output


    def set_starting_position(self, pos):
        self.starting_position = pos

    def get_geolocation_outcode_postcode(self, outcode_postcode):
        for row in self.outp:
            if outcode_postcode == row[1]:
                return row[2], row[3], str(row[2]) + '%2C' + str(row[3])

    def get_category_data(self, coord_postcode, miles, category, postcode):
        meters_in_a_mile = 1609
        coord = coord_postcode  # '51.5322%2C-0.6346'
        cat = category
        meters = meters_in_a_mile  * miles
        url = 'http://places.demo.api.here.com/places/v1/discover/explore?in=' + coord \
              + '%3Br%3D' + str(meters) \
              + '&cat=' + cat \
              + '&size=10000&Accept-Language=en-US%2Cen%3Bq%3D0.9%2Cen-GB%3Bq%3D0.8&' \
              + 'app_id=' + self.app_id + '&app_code=' + self.app_code

        url_req = requests.get(url)
        url_content = url_req.content
        js = json.loads(url_content)

        results_df = pd.DataFrame(columns=['postcode', 'category', 'distance_m', 'title', 'address', 'position'])
        if url_req.status_code == 200:
            c = 0
            for i in js['results']['items']:
                results_df.loc[c, ['postcode']] = postcode
                results_df.loc[c, ['category']] = (i['category']['id'])
                results_df.loc[c, ['distance_m']] = (i['distance'])
                results_df.loc[c, ['title']] = (i['title'])
                results_df.loc[c, ['address']] = str(i['vicinity']).replace('<br/>', ', ')
                results_df.loc[c, ['position']] = str(i['position'])
                c += 1

        return (results_df)

    def get_travel_time(self, waypoint0, waypoint0_name, waypoint1, waypoint1_name):
        url = "https://route.api.here.com/routing/7.2/calculateroute.json?" \
              "waypoint0="+str(waypoint0)+"&" \
              "waypoint1="+str(waypoint1)+"&" \
              "mode=fastest%3BpublicTransport&" \
              "combineChange=true&" \
              + 'app_id=' + self.app_id + '&app_code=' + self.app_code

        url_req = requests.get(url)
        url_content = url_req.content
        js = json.loads(url_content)

        # results_df = pd.DataFrame(columns=['starting_name', 'starting_coords', 'destination_name', 'destination_coords'], 'mins_taken', 'description')
        if url_req.status_code==200:
            mins_taken = js['response']['route'][0]['summary']['travelTime']
            try:
                mins_taken = round(mins_taken /60)
            except Exception as e:
                print(e)
            travel_description = js['response']['route'][0]['summary']['text']
            return [mins_taken, travel_description]
        return [-1,-1]



    def run(self):
        counter = 0
        for index, row in enumerate(self.outp):
            if index >= self.starting_position:
                postcode = row[1]
                postcode_geolocation = self.get_geolocation_outcode_postcode(postcode)


                # Get category data & write to file
                for c in self.categories:
                    c_f = open(self.base_folder + '/' + c + '.txt', 'a', encoding='utf8')
                    if counter > 0:
                        c_f.write(self.get_category_data(postcode_geolocation[2], 5, c, postcode).to_csv(sep='\t',
                                                                                                    encoding='utf-8',
                                                                                                    header=False))
                    else:
                        c_f.write(self.get_category_data(postcode_geolocation[2], 5, c, postcode).to_csv(sep='\t',
                                                                                                    encoding='utf-8',
                                                                                                    header=True))
                    c_f.close
                    print(postcode + '-' + c)
            counter += 1

        print('HereMapping Run Completes')