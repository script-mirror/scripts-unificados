import os
import sys
import pdb
import datetime
import logging


path_fontes = "/WX2TB/Documentos/fontes/"
sys.path.insert(1,path_fontes)
from PMO.scripts_unificados.bibliotecas.directories_management import *

now = datetime.datetime.now()
# now = now.strftime("%Y-%m-%d_%H:%M:%S")
now = now.strftime("%Y-%m-%d")

logDir = pwd()+'/logs/'
create_folder(logDir)

logging.basicConfig(filename=logDir+'log_'+str(now)+'.log',level=logging.WARNING, format='%(asctime)s : %(levelname)s : %(message)s')

def printLogs():
    logging.debug('Example of DEBUG message')
    logging.info('Example of INFO message')
    logging.warning('Example of WARNING message')
    logging.error('Example of ERROR message')
    logging.critical('Example of CRITICAL message')


if __name__ == '__main__':
    printLogs()



#############################
# import os
# import pdb
# import datetime
# import logging

# from logging.handlers import TimedRotatingFileHandler
# import re

# from directories_management import *

# logDir = pwd()+'/logs/'
# create_folder(logDir)

# # Create a custom logger
# logger = logging.getLogger(__name__)

# # Create handlers
# c_handler = logging.StreamHandler()
# # f_handler = logging.FileHandler('file.log')
# f_handler = TimedRotatingFileHandler(logDir+"current.log", when="midnight", interval=1)

# # add a suffix which you want
# f_handler.suffix = "%Y%m%d"
# #need to change the extMatch variable to match the suffix for it
# f_handler.extMatch = re.compile(r"^\d{8}$") 

# c_handler.setLevel(logging.INFO)
# f_handler.setLevel(logging.WARNING)


# # Create formatters and add it to handlers
# c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
# f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# c_handler.setFormatter(c_format)
# f_handler.setFormatter(f_format)

# # Add handlers to the logger
# logger.addHandler(c_handler)
# logger.addHandler(f_handler)


# def printLogs():
#     logger.debug('Example of DEBUG message')
#     logger.info('Example of INFO message')
#     logger.warning('Example of WARNING message')
#     logger.error('Example of ERROR message')
#     logger.critical('Example of CRITICAL message')


# if __name__ == '__main__':
#     printLogs()
