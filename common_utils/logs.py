import os
import json
import logging
import traceback
import functools
import datetime as dt
from dateutil import tz
from collections import deque
import os

_logs = deque()

def logger_config():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    os.makedirs('Logs', exist_ok=True)

    # now = dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S.%f')
    # path_to_file = os.path.join(folder, f'{now}-{os.getpid()}.json')

    file_path = f"Logs/{dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S')}-{os.getpid()}.log"
    file_handler = logging.FileHandler(file_path, encoding = "UTF-8")
    # file_handler = logging.FileHandler(f"{os.getcwd()}/Logs/logs-{dt.date.today().strftime('%Y-%m-%d')}.log",encoding = "UTF-8")
    # file_handler.setFormatter(formatter)
    # file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    # stream_handler.setFormatter(formatter)
    # stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    return logger
logger = logger_config()

def handle_error(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            result = function(*args, **kwargs)
        except Exception as exc:
            log_error(
                error_info(exc),
                **{
                    'function': f'{function.__module__}.{function.__name__}',
                    'function-args': args,
                    'function-kwargs': kwargs,
                },
            )
        else:
            return result
    return wrapper


def log_error(exc, **kwargs):
    '''Add an error event to the logs collection.
    
    Parameters
    ----------
        exc -- dict from error_info or Exception object
    
    Keyword Arguments
    -----------------
        Anything. They will get added to the log event.
    
    Response
    --------
        None
    
    Side Effects
    ------------
        Adds the error event to the internal _logs deque object
    '''
    
    if isinstance(exc, Exception):
        exc = error_info(exc)
    event = {**exc, **kwargs}
    _logs.append(event)


def log_event(**kwargs):
    '''Add an arbitrary event to the logs collection.
    
    Keyword Arguments
    -----------------
        Anything. They will get added to the log event.
    
    Response
    --------
        None
    
    Side Effects
    ------------
        Adds the event to the internal _logs deque object
    '''
    
    _logs.append(kwargs)


def error_info(exc):
    '''Return the error information in a structured format.
    
    Parameters
    ----------
        exc -- Exception object
    
    Response
    --------
        dict like this:
        {
            'type': 'ExceptionName',
            'args': ['args', 'passed', 'while', 'raising'],
            'tb': 'Traceback information...',
            'timestamp': '2020-01-01T01:01:01.000001+00:00',
        }
    '''
    
    return {
        'type': type(exc).__name__,
        'args': [*map(json_safe_representation, exc.args)],
        'tb': traceback.format_exc(),
        'timestamp': (dt.datetime.now(tz=tz.gettz()).astimezone(tz.tzutc()).isoformat()),
    }


def reset():
    '''Clear logs.'''
    _logs.clear()


def save():
    '''Save the accumulated logs in 'folder'.'''
    if _logs:
        os.makedirs('Logs', exist_ok=True)
        now = dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S.%f')
        path_to_file = os.path.join('Logs', f'{now}-{os.getpid()}.json')
        with open(path_to_file, 'w') as file:
            json.dump(_logs, file, indent=4, cls=LogsEncoder)


def json_safe_representation(obj):
    '''Return the same object if JSON serialisable, or its string version.'''
    JSON_SAFE_TYPES = (
        str,
        int,
        bool,
        float,
        type(None),
    )
    if isinstance(obj, JSON_SAFE_TYPES):
        return obj
    return str(obj)


class LogsEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, BaseException):
            return type(o).__name__
        if isinstance(o, deque):
            return list(o)
        
        try:
            return json.JSONEncoder.default(self, o)
        except TypeError:
            return str(o)
