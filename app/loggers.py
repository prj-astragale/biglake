#######################
# Logger setup from cfg
#######################
import logging
from logging.config import dictConfig
log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            # "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(levelprefix)s %(asctime)s %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",

        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    },
    "loggers": {
        "inlake-root":      {"handlers": ["default"], "level": "INFO"},
        "inlake-filesys":   {"handlers": ["default"], "level": "INFO"},
        "inlake-triples":   {"handlers": ["default"], "level": "INFO"},
        "inlake-ingress":   {"handlers": ["default"], "level": "INFO"},
        "inlake-kafka":     {"handlers": ["default"], "level": "INFO"},
    },
    
}
dictConfig(log_config)

logger_r = logging.getLogger('inlake-root')
logger_f = logging.getLogger('inlake-filesys')
logger_t = logging.getLogger('inlake-triples')
logger_k = logging.getLogger('inlake-kafka')
logger_i = logging.getLogger('inlake-ingress')
# s_log = logging.getLogger('inl-sch-logger')
# i_log = logging.getLogger('inl-ing-logger')