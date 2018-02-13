from lib import APIHandler

# File names for CSV writing
EIA_ELEC_DICT = "data/elec_demand_dictionary.csv"
EIA_ELEC_TABLE = "data/elec_demand_hourly.csv"

# Generate data for all APIHandler queries
def generate_all_data(api_sess):
    api_sess.gen_ba_demand_dictionary()
    api_sess.gen_ba_demand_table()

# Write all generated data to CSV
def write_all_data(api_sess):
    api_sess.write_ba_demand_dictionary(EIA_ELEC_DICT)
    api_sess.write_ba_demand_table(EIA_ELEC_TABLE)



