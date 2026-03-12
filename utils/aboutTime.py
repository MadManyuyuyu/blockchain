
import time
from datetime import datetime
def getCurrentTime():

    current_time_seconds = time.time()
    current_time_milliseconds = int(current_time_seconds * 1000)
    return str(current_time_milliseconds)

