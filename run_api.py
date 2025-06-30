from business_data_api.api import create_app

app = create_app()



#TODO
# Check tests
# Postgresql function for flask to fetch statuses of each document being scraped
# addiitonal check - we check if documents send to worker are scrpaed (done status in psotgresql), and if
# not yet scrpaed we make sure that the worker process is still running
# How to incorporate logging with the standard logging of fastapi?
# Incorporate logging into workers and server and add db route for storing logs

# TODO - KRSDF
# add function for checking if KRSDF Is not during maintenance



