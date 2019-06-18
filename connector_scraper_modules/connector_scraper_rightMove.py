from typing import TextIO

import pandas as pd
import json as json
from lxml import html, etree
import requests
import datetime
from connector_scraper_modules.scraper_rightMove import rightmove_data
from threading import Thread
import time
import queue
import codecs
import math
from pathlib import Path
import connector_scraper_modules.connector_scraper_hereMaps as Here


class RightMoveScrapper:
    text_encoding = 'utf-8'
    number_of_threads = 20
    bank_station_coords = '51.5134%2C-0.0890'
    canary_wharf_coords = '51.5039%2C-0.0186'



    def __init__(self, outcodes_file_location, output_filename, subset_filename, base_folder):
        self.outcodes_file_location = outcodes_file_location
        self.output_filename = output_filename
        self.matched_output_filename_json = output_filename + '_matched.json'
        self.output_filename_json = output_filename + '.json'
        self.output_file_location = base_folder / output_filename
        self.output_file_location_json = base_folder / self.output_filename_json
        self.output_filename_rental = 'rental_' + output_filename
        self.output_file_location_rental = base_folder / self.output_filename_rental
        self.subset_filename = subset_filename
        self.base_folder = base_folder
        self.output_clean_file_location =''
        self.complete_rm_sales_results = pd.DataFrame()
        self.complete_rm_rental_results = pd.DataFrame()
        self.output_final_result_filepath = Path(base_folder) / Path('right-move-final-result-'+datetime.date.today().strftime('%Y-%m-%d')+'.json' )

    def __get_outcode(self, postcode):
        # read outcode file
        outcodes_file = open(self.outcodes_file_location, 'r', encoding=self.text_encoding)
        outcodes_json = json.load(outcodes_file)
        outcode = ''

        for o in outcodes_json:
            if o['outcode'] == postcode:
                outcode = o['code']
        return outcode

    def __get_right_move_info(self, url_outcode, purchase_type):
        if purchase_type == 'sale':
            rightmove_purchase_type = 'property-for-sale'
        if purchase_type == 'rent':
            rightmove_purchase_type = 'property-to-rent'
        # connect to rightmove
        url = "https://www.rightmove.co.uk/" + str(rightmove_purchase_type) + "/find.html?locationIdentifier=OUTCODE%5E" + str(
            url_outcode) + "&insId=2&numberOfPropertiesPerPage=24&areaSizeUnit=sqft&googleAnalyticsChannel=buying"
        try:
            rightmove_object = rightmove_data(url)
            return rightmove_object
        except Exception as e:
            print('no results error ' + str(e))

    def __get_right_move_rental_info (self, outcode, postcode):
        rm_obj_rent = self.__get_right_move_info(outcode, 'rent')
        rm_df_rent = rm_obj_rent.get_results

        rm_df_rent.fillna(1)
        for j in rm_df_rent.index:
            rm_df_rent.loc[j, 'price'] = float(rm_df_rent.loc[j, 'price'])

        for i in rm_df_rent.index:
            try:
                rm_df_rent.number_bedrooms.loc[i] = float(rm_df_rent.number_bedrooms.loc[i])
            except Exception as e:
                print('Could not convert: ' + rm_df_rent.number_bedrooms.loc[i] + ' to float. ' + e)
                rm_df_rent.number_bedrooms.loc[i] = float(-1)

            if rm_df_rent.number_bedrooms.loc[i] == 0:
                rm_df_rent.number_bedrooms.loc[i] = float(1)

        rm_df_rent['cost_per_room'] = rm_df_rent.price / rm_df_rent.number_bedrooms
        rm_df_rent['outcode']= postcode
        return rm_df_rent

    def __get_right_move_info_all_postcodes(self, filelocation,rental_filelocation, outcodesfile, starting_index, subsetfile, capturePageData):
        outcodes_file = open(outcodesfile, 'r', encoding=self.text_encoding)
        outcodes_json = json.load(outcodes_file)
        if subsetfile != '':
            subset_file = open(subsetfile, 'r', encoding=self.text_encoding)
            subset_list = subset_file.readlines()
            for i, l in enumerate(subset_list):
                subset_list[i] = l.strip()

            temp_list = []
            for o in outcodes_json:
                if o['outcode'] in subset_list:
                    temp_list.append(o)
            outcodes_json = temp_list
            print(outcodes_json)
            print(len(outcodes_json))

        outcode = ''
        c = 0  # counter used for the header
        for o in outcodes_json:
            if o['code'] >= starting_index:
                outcode = o['code']
                postcode = o['outcode']
                try:
                    #------ CAPTURE RENTAL AND SALE DATA ---------
                    print('Starting ' + postcode + ' ' + str(datetime.datetime.today()))
                    rm_obj_sale = self.__get_right_move_info(outcode, 'sale')
                    # Sometimes Rightmove stops responding because of too many requests, so need to wait
                    if rm_obj_sale is None:
                        time.sleep(10)
                        rm_obj_sale = self.__get_right_move_info(outcode, 'sale')
                        if rm_obj_sale is None:
                            time.sleep(10)
                            rm_obj_sale = self.__get_right_move_info(outcode, 'sale')

                    rm_df_sale = rm_obj_sale.get_results
                    rm_df_rent = self.__get_right_move_rental_info(outcode, postcode)

                    print('---Captured results ' + postcode + ' (' + str(rm_obj_sale.results_count) + ') ' + str(
                        datetime.datetime.today()))

                    if capturePageData == True:
                        rm_df_sale = self.__get_addtional_page_data_with_threads(rm_df_sale,postcode)

                    f_sale: TextIO = open(filelocation, "a", encoding=self.text_encoding )
                    f_rent = open(rental_filelocation, 'a', encoding=self.text_encoding)

                    # confirm whether to print header
                    if c > 0:
                        f_sale.write(rm_df_sale.to_csv(sep='\t', header=False,  quotechar='^'))
                        f_rent.write(rm_df_rent.to_csv(sep='\t', header=False, quotechar='^'))
                    else:
                        f_sale.write(rm_df_sale.to_csv(sep='\t', quotechar='^'))
                        f_rent.write(rm_df_rent.to_csv(sep='\t', quotechar='^'))

                    f_sale.close
                    f_rent.close
                    c += 1

                    # print('printed: ' + str(rightmove_object.results_count) + ' records to ' + filename + '.txt')
                    print('printed: ' + str(rm_obj_sale.results_count) + ' records for: ' + postcode + ' outcode:  ' + str(
                        outcode))
                except Exception as e:
                    print('Error for ' + str(postcode) + ' outcode: ' + str(outcode) + ' ' + str(e))

    def __get_addtional_page_data_with_threads(self, df, postcode):
        q = queue.Queue()
        for i in df.index:
            q.put([i, (df.loc[i, 'url'])])
        print (q.qsize())

        threads = []
        start = time.time()
        for i in range(10):
            t = Thread(target=self.__get_additional_page_data, args=(q, postcode, df))
            threads.append(t)
            t.start()
        q.join()

        for t in threads:
            t.join()
            print(t.name, 'has joined')

        end = time.time()
        print('time taken: {:.4f}'.format(end - start))
        return df

    def __get_subtree_xpath(self, subtree, field_xpath, name_of_field, index):
        try:
            return subtree.xpath(field_xpath)
        except Exception as e:
            print('-----Unable to capture '+ name_of_field +' ' + str(index) + ' ' + str(e))

    def __get_additional_page_data(self, q, postcode, df):

        while not q.empty():

            try:
                q_values = q.get(block=False)
                index = q_values[0]
                url = (q_values[1])
                url_req = requests.get(url)
                url_content = url_req.content

                # Create subtree and xpaths of the page data
                subtree = html.fromstring(url_content)
                xp_key_features = """//ul[@class='list-two-col list-style-square']//text()"""
                xp_tenure = """//div[@class='sect ']/p/span[@id='tenureType']/text()"""
                xp_station_list = """//div[@class='right desc-widgets']/div[@class='clearfix nearest-stations']//ul[@class='stations-list']//li//*[local-name()='span' or local-name()='small']/text()"""
                xp_floor_plan = """//div[@id='floorplanTabs']//img/@src"""
                xp_prev_sold_info = """//tr[@class='bdr-b similar-nearby-sold-history-row-height']//td/text()"""
                xp_similar_sold_link = """//a[@id='soldPriceGoTo']/@href"""
                xp_location = """//div[@class='pos-rel']/a[@class='block js-tab-trigger js-ga-minimap']/img/@src"""

                # Create data lists from xptahs
                key_features = self.__get_subtree_xpath(subtree, xp_key_features, 'key features', index)
                tenure = self.__get_subtree_xpath(subtree, xp_tenure, 'tenure', index)
                station_list = self.__get_subtree_xpath(subtree, xp_station_list, 'station_list', index)
                floor_plan = self.__get_subtree_xpath(subtree, xp_floor_plan, 'floor_plan', index)
                prev_sold_info = self.__get_subtree_xpath(subtree, xp_prev_sold_info, 'prev_sold_info', index)
                similar_sold_link = self.__get_subtree_xpath(subtree, xp_similar_sold_link, 'similar_sold_link', index)
                location = self.__get_subtree_xpath(subtree, xp_location, 'location', index)

                df.loc[index, 'key_features'] = ' '.join(key_features).replace('\r\n            ', ',')
                df.loc[index, 'tenure'] = ' '.join(tenure)
                df.loc[index, 'station_list'] = ' '.join(station_list)
                df.loc[index, 'floor_plan'] = ' '.join(floor_plan).replace(' ', '')
                df.loc[index, 'prev_sold_info'] = ' '.join(prev_sold_info)
                df.loc[index, 'similar_sold_link'] = ' '.join(similar_sold_link)
                df.loc[index, 'location'] = ' '.join(location)
                df.loc[index, 'outcode'] = postcode
                print('-----Captured page data for ' + postcode + ' index:' + str(index))
                q.task_done()
            except Exception as e:
                print('-----Unable to capture URL from index ' + str(index) + ' ' + str(e))
                q.task_done()

        return df


    def run(self,starting_index):
        self.__get_right_move_info_all_postcodes(self.output_file_location, self.output_file_location_rental ,self.outcodes_file_location,starting_index,self.subset_filename, True)
        # ----- Update the internal dataframe with the latest results --------
        self.update_complete_rm_rental_results(self.output_file_location_rental)
        self.update_complete_rm_sales_results(self.output_file_location)
        print('Run Complete')

    def update_complete_rm_rental_results(self, file):
        self.complete_rm_rental_results = pd.read_table(file, lineterminator='\n', error_bad_lines=False, quotechar='^', encoding=self.text_encoding)

    def update_complete_rm_sales_results(self, file):
        self.complete_rm_sales_results = pd.read_table(file, lineterminator='\n',error_bad_lines=False, quotechar='^', encoding=self.text_encoding)

    def update_complete_rm_sales_results_json(self, file):
        with open(file) as recs:
            dict_recs = json.load(recs)

        # converting json dataset from dictionary to dataframe
        self.complete_rm_sales_results = pd.DataFrame.from_dict(dict_recs, orient='columns')

    def run_travel_time_analysis_with_threads(self, rm_sales_data):

        q = queue.Queue()
        for i in rm_sales_data.index:
            coords = str(rm_sales_data.loc[i, 'lat']) + '%2C' + str(rm_sales_data.loc[i, 'long'])
            id = str(rm_sales_data.loc[i, 'ID'])
            q.put([i,id,coords])

        threads = []
        start = time.time()
        for i in range(self.number_of_threads):
            t = Thread(target=self.run_travel_time_analysis, args=(rm_sales_data, q))
            threads.append(t)
            t.start()
        q.join()

        for t in threads:
            t.join()
            print(t.name, 'has joined')

        end = time.time()
        filename = self.base_folder / Path(
            'houses_for_sale_travel_time_analysis_' + datetime.datetime.today().strftime('%Y_%m_%d_%H%M') + '.json')
        self.write_to_json(rm_sales_data, filename)
        self.complete_rm_sales_results = rm_sales_data
        print('time taken: {:.4f}'.format(end-start))
        return rm_sales_data

    def run_travel_time_analysis(self, rm_sales_data, q):
        here_api = Here.HereMapping('', self.base_folder)
        number_of_records = len(rm_sales_data.index) - 1

        while not q.empty():
            try:
                q_values = q.get(block=False)
                index = q_values[0]
                id = q_values[1]
                coords = q_values[2]

                rm_sales_data.loc[index, 'bank_station_travel_time'] = \
                here_api.get_travel_time(coords, id, self.bank_station_coords, 'Bank Station')[0]
                rm_sales_data.loc[index, 'canary_wharf_station_travel_time'] = \
                here_api.get_travel_time(coords, id, self.canary_wharf_coords, 'Canary Wharf Station')[0]
                print('-- Capturing travel time data ' + str(index) + ' of ' + str(number_of_records))
                q.task_done()
            except Exception as e:
                print ('-- Error in capturing travel time data', index, 'of', number_of_records, 'coords:', coords, 'e:', e)
                time.sleep(5)
                q.task_done()

        return rm_sales_data

    def clean(self):
        self.c = self.Clean(self.base_folder, self.complete_rm_rental_results, self.complete_rm_sales_results)
        self.complete_rm_sales_results = self.c.run()
        self.output_clean_file_location = self.c.export_file_name
        self.write_to_json(self.complete_rm_sales_results,self.output_clean_file_location)
        # self.update_complete_rm_sales_results( self.output_clean_file_location)
        print('Clean Complete')

    def write_to_json(self, df, filepath):
        df = df.drop(df.columns[df.columns.str.contains('unnamed', case=False)], axis=1)
        df.to_json(path_or_buf=filepath, orient='records')
        print('Json exported')

    def write_to_file(self, df, filepath):
        df.to_csv(filepath, sep='\t', chunksize=1, encoding=self.text_encoding)
        print('done')
        print('Export complete -'+ str(datetime.datetime.today()))

    class Clean:
        lat_pos = 47
        lon_pos = 67


        def __init__(self, base_folder, df_rental, df_sales): #right_move_file_location, right_move_file_location_rental):
            self.base_folder = base_folder
            # self.filename = right_move_file_location
            # self.rental_file_location = right_move_file_location_rental
            # only import rows without errors (i.e. major alingment issues)
            self.df =  df_sales  #pd.read_table(right_move_file_location, error_bad_lines=False)
            self.df_rental = df_rental   #pd.read_table(right_move_file_location_rental, error_bad_lines= False)
            self.export_file_name = self.base_folder / Path('houses_for_sale_data_clean_' + datetime.datetime.today().strftime('%Y_%m_%d_%H%M') + '.json')
            #     Clean columns
            self.df_rental.columns = self.df_rental.columns.str.replace('\r', '')
            self.df.columns = self.df.columns.str.replace('\r', '')


        def write_to_file(self):
            f = codecs.open(self.export_file_name , 'w', encoding=self.text_encoding)
            f.write(self.df.to_csv(sep='\t'))
            f.close

        def __add_ids(self):
            ids = list(range(0, len(self.df)))
            self.df['ID'] = ids
            print('Capturing RM IDs complete ' + str(datetime.datetime.today()))
            return self.df

        def set_export_full_file_path(self, full_file_path):
            self.export_file_name = full_file_path

        def __get_long_lat_from_rm_data(self):
            self.df = self.df.fillna('')
            self.df['lat'] = self.df['location'].str.findall('(-?\d+.{1}\d{3,})').str[0]
            self.df['long'] = self.df['location'].str.findall('(-?\d+.{1}\d{3,})').str[1]

            #clean up long lat
            self.df = self.df[(self.df.lat != '') & (self.df.long != '')]
            self.df['lat'] = [float(x) for x in self.df['lat']]
            self.df['long'] = [float(x) for x in self.df['long']]
            self.df['lat_radians'] = self.df['lat'] * math.pi / 180
            self.df['long_radians'] = self.df['long'] * math.pi / 180
            print('Capturing RM long-lat data complete \t' + str(datetime.datetime.today()))

            return self.df

        def __clean_price(self):
            try:  # Need the try because where there are no blanks the below code will result in an error
                self.df['price'][self.df.price == ''] = 0
            except Exception as e:
                print('No blanks to convert to 0. ' + str(e))
            self.df['price'] = [float(x) for x in self.df['price']]
            return self.df

        def run(self):
            # Check for incorrect alignment based on the Search Date column
            self.df['isDate'] = pd.to_datetime(self.df['search_date'], errors='coerce')
            self.df = self.df.dropna(subset=['isDate'])
            self.df = self.__add_ids()
            self.df = self.__get_long_lat_from_rm_data()
            self.df = self.__clean_price()
            self.df = self.__add_rental_info()
            self.df = self.__get_bathrooms()
            self.df = self.__get_sq_space()
            self.df = self.__clean_outcodes()

            #drop any unnamed columns
            self.df = self.df.drop(self.df.columns[self.df.columns.str.contains('unnamed', case=False)], axis=1)

            return self.df

        def __clean_outcodes(self):
            self.df.outcode = self.df.outcode.str.replace('\r','')
            return self.df

        def __add_rental_info(self):
            df_rental_reduced = self.df_rental[['address','outcode','cost_per_room']]
            df_rental_outcode_average = pd.DataFrame(df_rental_reduced.groupby(['outcode']).cost_per_room.mean().reset_index())
            df_rental_outcode_average.outcode = df_rental_outcode_average.outcode.str.replace('\r', '')
            return pd.merge(self.df, df_rental_outcode_average, left_on='outcode', right_on='outcode')\
                .rename(index=str, columns={'cost_per_room': 'outcode_average_cost_per_room_2'})

        def __get_bathrooms(self):
            self.df['bathrooms'] = self.df.descriptions.str.extract('(\w+ (?:bathroom))')
            self.df['bathrooms'] = self.df['bathrooms'].replace({'a ': 'one ', 'one': 1, 'two':2, 'three':3, 'four':4, 'five': 5, 'six':6, 'seven': 7}, regex=True)
            return self.df

        def __get_sq_space(self):
            self.df['sq_space'] = self.df.descriptions.str.extract('((\w+)(,?)\w+ (?:(S|s)q)(\.?) ?\w+)')[0]
            self.df['sq_space'] = self.df.sq_space.replace({'S':'s', 'q.':'q','F':'f', 'sqft': 'sq ft'}, regex=True)
            return self.df