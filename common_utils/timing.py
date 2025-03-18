
import time
import functools
from common_utils.logs import logger

def time_this(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        time_start = time.perf_counter()

        # Initialize variables for record identification
        record, classObj, nametup = None, None, None
        source_count = 0

        for i in args:
            if isinstance(i, tuple) and hasattr(i, "_fields"):  # Check if namedtuple
                nametup = i
                source_count += 1
            elif hasattr(i, "__class__"):  # Check if it's a class instance
                classObj = i

        if classObj is not None and nametup is None:
            record = getattr(classObj, "record", None)  # Safely get 'record' attribute
        else:
            record = nametup

        output = function(*args, **kwargs)
        time_end = time.perf_counter()
        # print(record)
        # Get target table name safely
        # target_table = getattr(args[0], "targettablename", "UnknownTable")
        if hasattr(record,"targettablename"):
            logger.info("Finished %s in %.2f seconds", record.targettablename, (time_end - time_start))
        elif hasattr(record,"targetobject"):
            logger.info("Finished %s in %.2f seconds", record.targetobject, (time_end - time_start))           
        else:
            logger.info("Finished in %.2f seconds",(time_end - time_start))
        return output
    return wrapper
