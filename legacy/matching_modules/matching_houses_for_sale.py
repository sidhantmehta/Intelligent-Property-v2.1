import matching_modules.matching_engine_module as Matching

def run_matching_engine(output_folder, reference_folder, complete_rm_sales_results):
    m = Matching.Matching_engine(output_folder,reference_folder, complete_rm_sales_results,[0,1,3,5,10], ['less_than_1', 'less_than_3', 'less_than_5,','less_than_10'] )
    m.run()
    m.write_to_json(m.results_consolidated, m.result_file_path)
    return m.results_consolidated
