from pathlib import Path
'''------Conn modules----'''
import connector_scraper_modules.connector_scraper_rightMove as cs_rm
import connector_scraper_modules.connector_scraper_hereMaps as cs_hm
'''------Ref modules----'''
import reference_modules.reference_houses_for_sale as ref_sale_module
'''------Matching modules----'''
import matching_modules.matching_houses_for_sale as match_sale_module
'''------Other----'''
import time
import datetime
import pandas as pd
start = time.time()

'''-----------------------SETTINGS START-------------------'''
text_encoding = 'utf-8'
outcodes_file_location = Path('connector_scraper_data/outcodes.txt')
output_filename = 'houses_for_sale_data_preclean_'+datetime.datetime.today().strftime('%Y_%m_%d_%H%M')+'.txt'
subset_filename =Path('connector_scraper_data/outcodes_debug_London_zone1_zone2.txt')
base_folder = Path('staging_data/')
postcode_outcode_file_path= Path('connector_scraper_data/debug_postcode_outcodes_with_longlat.csv')
# manual_file_sales = Path('matching_data/houses_for_sale_data_complete_2019_02_04.txt')
# manual_file_rental = Path('staging_data/rental_houses_for_sale_data_preclean_2019_02_04_0119.txt')
'''---------------------------------------------------'''



'''-----------------------STAGING START-------------------'''

# hm = cs_hm.HereMapping(postcode_outcode_file_path,base_folder / Path('/Data/'))
# hm.run()

rms = cs_rm.RightMoveScrapper(outcodes_file_location,output_filename, subset_filename, base_folder)
rms.run(0)

# MANUAL INPUTS
# rms.update_complete_rm_sales_results_json(manual_file_sales)  #(rms.output_file_location)
# rms.update_complete_rm_rental_results(manual_file_rental) #rms.output_file_location_rental)

rms.clean()
rms.run_travel_time_analysis_with_threads(rms.complete_rm_sales_results)

'''-----------------------REFERENCE START-------------------'''
sales_results = ref_sale_module.run_reference_modules(rms.complete_rm_sales_results)


'''-----------------------MATCHING START-------------------'''
sales_results = match_sale_module.run_matching_engine(Path('matching_data'),Path('reference_data'), sales_results)


sales_results.reset_index()
rms.write_to_json(sales_results, Path('matching_data/houses_for_sale_data_complete_'+datetime.date.today().strftime('%Y_%m_%d')+'.txt' ))

end = time.time()
print('time taken: {:.4f} mins'.format((end - start)/60))

