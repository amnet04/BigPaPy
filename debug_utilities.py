import psutil
import resource
import inspect
import logging

logging.basicConfig(filename="/tmp/bigpapy.log",
                    level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')


def secure_limits():
    # Calculate secure memory parameters
    available_memory = psutil.virtual_memory().available  # (/1048576 for MB)
    secure_memory = int(available_memory / 8)
    return (available_memory, secure_memory)


def duration_log(start, end, notes=""):
    func = inspect.currentframe().f_back.f_code
    duration = end - start
    message = ("{}-{}-{} \t {}hh, {}mm, {}ss, >>> {}".format(
        func.co_filename,
        func.co_name,
        func.co_firstlineno,
        int(duration / 3600),
        int(duration % 3600 / 60),
        duration % 3600 % 60,
        notes
    ))

    logging.info(message)


def mem_log(message=""):
    func = inspect.currentframe().f_back.f_code
    message = "{}: {}: {} \t {} Mb {}".format(
        func.co_name,
        func.co_filename,
        func.co_firstlineno,
        int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024),
        message
    )

    logging.debug(message)


def gen_log(message):
    func = inspect.currentframe().f_back.f_code
    message = "{}: {}: {} \t {}".format(
        func.co_name,
        func.co_filename,
        func.co_firstlineno,
        message
    )

    logging.info(message)
