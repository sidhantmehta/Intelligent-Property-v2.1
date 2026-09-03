import requests as req
from pathlib import Path
import time
import pandas as pd
import numpy as np
desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',20)
from robobrowser import RoboBrowser
from pdf2image import convert_from_path
import pytesseract as pt
import re
import datetime


'''
1) Get a table of addresses to get EPC for
2) search EPC website for address
3) download the pdf
4) use ocr to capture the property size
'''


class epc:

    def __init__(self, basefolder, landregistry_data, postcode_filter):
        # Settings
        self.results_filepath = basefolder / Path('epc_results_' + datetime.datetime.today().strftime('%y-%m-%d-%H%M') + '.txt')
        self.pdf_filename = basefolder / Path('temp.pdf')
        self.land_reg_df = landregistry_data

        # CONSTANTS
        self.start = time.time()
        self.epcSqm = []
        self.epcLink = []
        self.epcAddress = []
        self.epcCols = ['address', 'link', 'sqm']
        self.browser = RoboBrowser(history=True, parser='lxml')  # specify parser to overcome the warning message
        self.filtered_list = self.land_reg_df[self.land_reg_df.postcode.str.contains(postcode_filter, regex=False)].postcode.unique()
        print('Sample of filtered list of postcodes:', self.filtered_list[0:10])

    def get_epc_size_data(self):

        for index, postc in enumerate(self.filtered_list):
            print('Scanning', postc, ': ', index, 'Out of', self.filtered_list.size, 'postcodes')
            self.captureEPCReports(postc)
            # Put the results into a DF
            results_df = pd.DataFrame(dict(list(zip(self.epcCols, [self.epcAddress, self.epcLink, self.epcSqm]))))
            results_df.sqm = results_df.sqm.astype(float)
            # Append results to CSV
            self.append_to_csv(self.results_filepath, results_df)

        end = time.time()
        print('time taken: {:.4f}'.format((end - self.start) / 60))

    def append_to_csv (self, filepath,df):
        with open(self.results_filepath, 'a') as file:
            df.to_csv(file, sep='\t')

    def captureSqmFromEPC(self, url):
        epc_url = self.browser.url
        response = req.get(epc_url)  # run the url
        self.pdf_filename.write_bytes(response.content)  # write the pdf to disk

        pages = convert_from_path('temp.pdf', 300)
        pages[0].save('temp.jpg', 'JPEG')
        pdf_text = pt.image_to_string('temp.jpg', lang='eng')
        sqm = re.search('((Total floor area: )+\d+|\d+ m/?)', pdf_text)
        # Capture the line Total floor area...
        try:
            sqm = re.search('\d+', sqm[0])
        except Exception:
            print('Error getting sqm', Exception)

        return sqm[0]


    def captureEPCReports(self, postcode):
        epc_postcode = postcode

        # Retrieve webpage
        self.browser.open('https://www.epcregister.com/reportSearchAddressByPostcode.html')
        accept_form = self.browser.get_forms()[0]  # get the postcode search form
        self.browser.submit_form(accept_form, accept_form['accept'])  # accept the notice
        postcode_form = self.browser.get_forms()[0]
        postcode_form['postcode'].value = epc_postcode

        self.browser.submit_form(postcode_form)
        address_links = self.browser.get_links()

        address_dict = {}  # Capture only the links relevant to house reports
        for a in self.browser.get_links():
            if a.text.find(epc_postcode) > 0:
                address_dict[a] = a.text

        for index, link in enumerate(address_dict.keys()):
                print('Scanning', index, 'out of', len(address_dict.keys()), 'houses')
                self.browser.follow_link(link)  # follow the link of an address line
                self.browser.submit_form(self.browser.get_forms()[0])  # follow the downlaod report form
                self.browser.follow_link(self.browser.get_links()[17])  # follow the Click here to view report (PDF) link

                # Capture PDF
                sqm = self.captureSqmFromEPC(self.browser.url)  # get URL of the epc pdf
                self.epcSqm.append(sqm)
                self.epcAddress.append(address_dict[link])
                self.epcLink.append(link)
                #squareMeterResults.append(np.array([address_dict[link],s]))
                print(address_dict[link], sqm)  # print out the address and the sqm
                time.sleep(np.random.randint(2,5))
