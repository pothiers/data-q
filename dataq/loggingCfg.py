# Example! Not used!!!
# From: https://docs.python.org/3/howto/logging-cookbook.html?highlight=rotating#an-example-dictionary-based-configuration

import socket 
import logging.handlers 

LOG_SETTINGS = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },
    #! 'filters': {
    #!     'special': {
    #!         '()': 'project.logging.SpecialFilter',
    #!         'foo': 'bar',
    #!     }
    #! },
    'handlers': {
        #!'null': {
        #!    'level':'DEBUG',
        #!    'class':'django.utils.log.NullHandler',
        #!},
        #!'email': {
        #!    'level': 'ERROR',
        #!    'class': 'logging.handlers.SMTPHandler',
        #!    'mailhost': 'email.myCompany.com',
        #!    'fromaddr': 'fred@myCompany.com',
        #!    'toaddrs': ['barney@myCompany.com'],
        #!    'subject': 'Error from DATAQ',
        #!},
        'console':{
            #!'level':'DEBUG',
            'class':'logging.StreamHandler',
            'formatter': 'simple',
        },
        'syslog': {
            'level': 'DEBUG',
            'class': 'logging.handlers.SysLogHandler',
            'formatter': 'verbose',
            #!'socktype': socket.SOCK_STREAM,
            'address': '/dev/log',
            #
            # Put so mething like the following in /etc/rsyslog.conf
            #   local1.*	/var/log/dataq/dataq.log
            'facility': logging.handlers.SysLogHandler.LOG_LOCAL1,
        },
        'file': {
            'level': 'WARNING',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'simple',
            'filename': '/var/dataq/logs/dataq_cli.log',
            'maxBytes': 4096,
            'backupCount': 9,
        },
    },
    'loggers': {
        'dataq.cli': {
            #! 'handlers': ['console', 'syslog'],
            'handlers': ['console'],
            #! 'handlers': ['syslog'],
            #! 'handlers': ['file'],
            'level': 'WARNING',
        }
    }
}
