import pandas as pd
import numpy as np
desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',20)

class landregistry:
    landregistry_clean_df = pd.DataFrame()

    def __init__(self, type):
        if type.upper() == 'M':
            self.landregistry_clean_df = self.getMonthlyData()
        elif type.upper() == 'A':
            self.landregistry_clean_df = self.getAllData()


    def getMonthlyData(self):
        link = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update.txt'
        return self.downloadAndCleanData(link)


    def getAllData(self):
        link = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.txt'
        return self.downloadAndCleanData(link)

    def downloadAndCleanData(self, link):
        land_reg_df= pd.read_csv(link, header=None)
        land_reg_df.columns = ['transaction_id', 'price', 'date_of_transfer', 'postcode', 'property_type', 'old_new_build',
                               'tenure', 'address_line_1', 'address_line_2', 'street', 'locality', 'town_city', 'district',
                               'county', 'ppd_category', 'record_status']
        land_reg_df = land_reg_df.fillna('')  # clean up
        land_reg_df['top_line_address'] = (land_reg_df.address_line_1.astype(str) + ',' + land_reg_df.address_line_2.astype(
            str) + ',' + land_reg_df.street.astype(str))
        land_reg_df.top_line_address = land_reg_df.top_line_address.str.replace(',,', ',')  # clean up
        return land_reg_df

    def write_to_json(self, filepath):
        self.landregistry_clean_df.to_json(filepath)

# lr = landregistry('M')
# print(lr.landregistry_clean_df.head())
