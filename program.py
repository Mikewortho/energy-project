from lib import APIHandler
from lib import DataManager

# API keys definition
EIA_API_KEY = "09ec6a933cda49f4ecb9fa3911c3bef2"

# Define APIHandler object
api_session = APIHandler.APIHandler(EIA_API_KEY)

# GenerateData functions
DataManager.generate_all_data(api_session)
DataManager.write_all_data(api_session)


