import pandas as pd
import datetime
import math
import numpy as np
from pathlib import Path
import dask.dataframe as dd


class Matching_engine:
    category_weighting = {'airport': 2, 'eat': 4, 'public_transport': 5, 'railway': 5, 'recreation': 2}
    text_encoding = 'utf-8'
    def __init__(self, output_folder, reference_folder, df_rm_sales, radius_bins, radius_bin_labels):
        print('Matching class started ' + str(datetime.datetime.today()))
        self.output_folder = output_folder
        self.cross_join_filepath = self.output_folder / Path(
            'houses_for_sale_crossj_' + datetime.datetime.today().strftime('%Y_%m_%d_%H%M') + '.txt')
        # self.file_loc_rm_data = file_loc_rm_data
        self.result_file_path = self.output_folder / Path(
            'houses_for_sale_data_results_' + datetime.datetime.today().strftime('%Y_%m_%d_%H%M') + '.txt')

        self.file_loc_eat_data = reference_folder / 'eat-drink.txt'
        self.file_loc_airport_data = reference_folder / 'airport.txt'
        self.file_loc_public_transport_data = reference_folder / 'public-transport.txt'
        self.file_loc_railway_data = reference_folder / 'railway-station.txt'
        self.file_loc_recreation_data = reference_folder / 'recreation.txt'

        self.set_radius_weightings(radius_bins, radius_bin_labels)

        # file = open(file_loc_rm_data, 'r', encoding=text_encoding)
        self.rm_data = df_rm_sales
        print('Capturing houses for sale data load compelte ' + str(datetime.datetime.today()))

    def set_category_weightings(self, airport, eat, pub_trans, railway, rec):
        self.category_weighting = {'airport': airport, 'eat': eat, 'public_transport': pub_trans, 'railway': railway,
                                   'recreation': rec}

    def set_radius_weightings(self, bins, bin_lables):
        self.bins = bins
        self.miles_radius_weightings_bins = [1 / bins[1], 1 / bins[2], 1 / bins[3], 1 / bins[4]]
        self.bin_labels = bin_lables
        print('Creating weightings tables complete. ' + str(datetime.datetime.today()))

    def __merge_geo_data(self):
        file = open(self.file_loc_airport_data, 'r', encoding=self.text_encoding)
        airport_df = pd.read_csv(file, delimiter='\t', encoding=self.text_encoding)
        airport_df['category_weighting'] = self.category_weighting['airport']
        airport_df['source_category'] = 'Airport'

        file = open(self.file_loc_eat_data, 'rb')
        eat_df = pd.read_csv(file, delimiter='\t')
        eat_df['category_weighting'] = self.category_weighting['eat']
        eat_df['source_category'] = 'Eat'

        file = open(self.file_loc_public_transport_data, 'rb')
        public_transport_df = pd.read_csv(file, delimiter='\t')
        public_transport_df['category_weighting'] = self.category_weighting['public_transport']
        public_transport_df['source_category'] = 'Public Transport'

        file = open(self.file_loc_railway_data, 'rb')
        railway_df = pd.read_csv(file, delimiter='\t')
        railway_df['category_weighting'] = self.category_weighting['railway']
        railway_df['source_category'] = 'Railway'

        file = open(self.file_loc_recreation_data, 'rb')
        recreation_df = pd.read_csv(file, delimiter='\t')
        recreation_df['category_weighting'] = self.category_weighting['recreation']
        recreation_df['source_category'] = 'Recreation'

        frames = [airport_df, eat_df, public_transport_df, railway_df, recreation_df]
        self.all_geo_df = pd.concat(frames)
        print('Merging of geo data complete ' + str(datetime.datetime.today()))

        # clean up lat long
        self.all_geo_df['cat_lat'] = self.all_geo_df['position'].str.slice(1, 8).str.replace(',', '').str.replace('-',
                                                                                                                  '')
        self.all_geo_df['cat_lat'] = [float(i) for i in self.all_geo_df['cat_lat']]
        self.all_geo_df['cat_long'] = self.all_geo_df['position'].str.slice(10, 19).str.replace(']', '').str.replace(
            ',', '')
        self.all_geo_df['cat_long'] = [float(i) for i in self.all_geo_df['cat_long']]
        # convert to radians
        self.all_geo_df['cat_lat_radians'] = self.all_geo_df['cat_lat'] * math.pi / 180
        self.all_geo_df['cat_long_radians'] = self.all_geo_df['cat_long'] * math.pi / 180

        print('Clean up geo df complete ' + str(datetime.datetime.today()))

    def __create_cross_join(self):
        # cross join with the rm data
        self.results_consolidated = pd.DataFrame()
        outcodes_list = self.rm_data.outcode.unique()
        outcodes_len = len(outcodes_list)
        for index, outcode in enumerate(outcodes_list):
            filtered_df = self.rm_data[self.rm_data.outcode == outcode]
            self.crossj = dd.merge(self.all_geo_df, filtered_df, left_on='postcode',
                                   right_on='outcode')
            # self.write_to_file(self.crossj, 'crossj.txt')
            # print('CrossJoin table created ' + str(datetime.datetime.today()))
            self.crossj['distance_miles'] = self.__getDistance(self.crossj.cat_long_radians,
                                                               self.crossj.cat_lat_radians,
                                                               self.crossj.long_radians, self.crossj.lat_radians)
            # print('Get Distance function completed' + str(datetime.datetime.today()))
            self.crossj['radius_group'] = pd.cut(self.crossj['distance_miles'], bins=self.bins, labels=self.bin_labels)
            # print('Radius Group created ' + str(datetime.datetime.today()))
            self.crossj['radius_weighting'] = pd.cut(self.crossj['distance_miles'], bins=self.bins,
                                                     labels=self.miles_radius_weightings_bins)
            # print('Radius weightings created ' + str(datetime.datetime.today()))
            self.crossj['total_category_score'] = self.crossj.radius_weighting.multiply(self.crossj.category_weighting,
                                                                                        axis='rows')
            # print('Total Category Score created ' + str(datetime.datetime.today()))

            # f_cj: TextIO = open(self.cross_join_filepath, "a", encoding=text_encoding)
            # f_cj.write(self.crossj.to_csv(header=True, sep='\t', chunksize=1))
            # f_cj.close()

            self.results_df = pd.DataFrame(
                self.crossj.groupby(['ID']).total_category_score.sum().reset_index())  # CHECK ALL FLOWING THROUGH
            self.results_df = self.results_df.merge(self.rm_data, on='ID', suffixes=('results_ID', 'rm_ID'))

            self.results_consolidated = pd.concat([self.results_consolidated, self.results_df])
            print(index, 'of', outcodes_len)
        # self.write_to_file(self.crossj, self.base_path / Path('right_move_geo_data_'+ datetime.date.today().strftime('%Y-%m-%d') +'.txt'))
        print('Cross joining complete. ' + str(datetime.datetime.today()))
        self.results_consolidated.rename(
            columns={'total_category_score': 'total_house_score_2'}, inplace=True)
        self.results_consolidated['total_house_score_2'] = (
                                                                   self.results_consolidated.total_house_score_2 - self.results_consolidated.total_house_score_2.min()) / (
                                                                   self.results_consolidated.total_house_score_2.max() - self.results_consolidated.total_house_score_2.min())
        print('Result normalization complete . ' + str(datetime.datetime.today()))
        print('Elements ' + str(len(self.results_consolidated)))

    def __getDistance(self, lon1, lat1, lon2, lat2):
        # This is the haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        # Need to convert the values to float64 to stop numpy throwing an error when performing sin and cos
        dlon = np.array(dlon.values).astype(np.float64)
        dlat = np.array(dlat.values).astype(np.float64)
        lat1 = np.array(lat1.values).astype(np.float64)
        lat2 = np.array(lat2.values).astype(np.float64)

        a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(
            dlon / 2.0) ** 2  # NEED to get this to work with sin

        c = 2 * np.arcsin(np.sqrt(a))
        km = 6367 * c
        miles = km * 0.621371
        return miles

    def __create_results_table(self):
        # create results table
        # self.results_df = pd.DataFrame(self.crossj.groupby(['ID']).total_category_score.sum().reset_index()) #CHECK ALL FLOWING THROUGH
        # print('Creating Resultlts Datafram table complete. ' + str(datetime.datetime.today()))
        # self.results_df.columns = ['ID', 'total_house_score']

        self.results_consolidated = self.results_consolidated.rename(
            columns={'total_category_score': 'total_house_score'}, inplace=True)
        self.results_consolidated['total_house_score'] = (
                                                                     self.results_df.total_house_score - self.results_df.total_house_score.min()) / (
                                                                     self.results_df.total_house_score.max() - self.results_df.total_house_score.min())
        print('Result normalization complete . ' + str(datetime.datetime.today()))
        # print('Elements before merge with ID table ' + str(len(self.results_df)))
        # self.results_df = self.results_df.merge(self.rm_data, on='ID', suffixes=('results_ID', 'rm_ID'))
        # print('Elements after merge with ID table ' + str(len(results_df)))
        # print('Merging Results and CrossJ table complete. ' + str(datetime.datetime.today()))
        print('Elements ' + str(len(self.results_df)))

    def write_to_file(self, df, filepath):
        df.to_csv(filepath, sep='\t', chunksize=1)
        print('done')
        print('Export complete -' + str(datetime.datetime.today()))


    def run(self):
        self.set_radius_weightings(self.bins, self.bin_labels)
        self.__merge_geo_data()
        self.__create_cross_join()
        # self.__create_results_table()

    def write_to_json(self, df, filepath):
        df = df.drop(df.columns[df.columns.str.contains('unnamed', case=False)], axis=1)
        df.to_json(path_or_buf=filepath, orient='records')
        print('Json exported')
