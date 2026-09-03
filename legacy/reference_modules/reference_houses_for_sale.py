import reference_modules.references_scenario_module as Scenario

def run_reference_modules(complete_rm_sales_results):
    s = Scenario
    s.run_scenario(complete_rm_sales_results, 0.1, 150, 2000, 40000, 35, 2.49)
    return complete_rm_sales_results