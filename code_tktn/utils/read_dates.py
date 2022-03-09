import sys
from datetime import datetime
import datetime as DATETIME
from constans import sales_constants

def read_specific_dates():
    args_dataproc = sys.argv
    specific_date = args_dataproc[1]
    specific_date_with_format = datetime.strptime(specific_date, sales_constants.date_format)
    specific_next_date = specific_date_with_format + DATETIME.timedelta(days=1)
    specific_next_date_with_format = specific_next_date.strftime(sales_constants.date_format)
    return specific_date_with_format.strftime(sales_constants.date_format),specific_next_date_with_format