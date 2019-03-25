import logging
import logging.config
import os

LOGGING_CONFIG = {
    'version': 1,  # required
    'disable_existing_loggers': False,  # this config overrides all other
    # loggers
    'formatters': {
        'simple': {
            'format': '%(asctime)s %(levelname)s -- %(message)s'
        },
        'whenAndWhere': {
            'format': '%(asctime)s\t%(levelname)s -- %(filename)s:%(lineno)s -- %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'whenAndWhere',
        },
    },
    'loggers': {
        # '': {  # 'root' logger
        #     'level': 'CRITICAL',
        #     'handlers': ['console'],
        # },
        'ot': {  # 'root' logger
            'level': 'INFO',
            'handlers': ['console'],
        },
    }
}

def get_logger():
    logging.config.dictConfig(LOGGING_CONFIG)
    return logging.getLogger('ot')

def get_error_file_logger(err_file):
    if os.path.isfile(err_file):
        os.remove(err_file)

    error_log_config = LOGGING_CONFIG
    error_log_config['handlers']['file'] = {
        'level': 'DEBUG',
        'class': 'logging.FileHandler',
        'formatter': 'whenAndWhere',
        'filename': err_file
    }
    error_log_config['loggers']['ot_error'] = {
        'level': 'ERROR',
        'handlers': ['console', 'file'],
    }

    logging.config.dictConfig(error_log_config)
    return logging.getLogger('ot_error')

