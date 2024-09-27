#######################################################################################
# 										WX Energy
#
# Description: Lib to manage files/directories
#
# Author: Thiago Scher (thiago.scher@gmail.com)
# Date: 07/09/2019
#
#######################################################################################

import os
import logging
import shutil
import pdb


def pwd():
	""" Get the present working directory 
	:return: Present working directory path
	"""
	return os.getcwd()
	# return os.path.abspath(os.path.dirname(__file__))

def create_tmp_pwd():
	""" Create a temporary folder in pwd (present working directory)
	:return: A string specifying where the tmp folder was created
	"""

	# Get the present working directory
	path = pwd() + '/tmp'
	return create_folder(path)


def create_folder(path):
	""" Create a directory in a specific path if it doesn't exist
	:param path: Path where the folder is going to be created
	:return: The path for the new folder
	"""

	# Create the directory if it doesn't exist
	if not os.path.exists(path):
		os.makedirs(path)
		return path


def create_file_copy(from_path, to_path, file_name=''):
	""" Create a file/directory copy
	:param from_path: Path of the original file
	:param to_path: Path where the copy is going to be created
	:param file_name: The file name of the copy
	:return: Error or success message
	"""

	# if to_path[-1] not in ['/', '\\']:
	# 	to_path = to_path+'/'


	# # Check if the file name was not specified 
	# if file_name == '':

	# 	# If it was specified, the original name is used
	# 	from_path = from_path.replace('\\','/')
	# 	file_name = from_path.split('/')[-1]

	try:
		shutil.copy(from_path, to_path)
		return 'Copy created with success ['+to_path+']'
	except Exception as e:
		print('Not possible to create a copy to '+to_path)
		print(e)

def move_file(from_path, to_path):
	""" Move a file/directory
	:param from_path: Original path of the file
	:param to_path: New path of the file
	:return: Error or success message
	"""

	file_name = from_path.replace('\\','/').split('/')[-1]

	# check if to_path is a file or directory
	if '.' not in to_path:
		# check if the to_path exist, if it doesn't, it is created
		if not os.path.exists(to_path):
			os.makedirs(to_path)

	try:
		shutil.move(from_path, to_path)
		return file_name + " moved to " + to_path 
	except Exception as e:
		print(e)

def delete_file(path):
	""" Delete a file/directory
	:param path: Path of the file/directory that is going to be deleted
	:return:
	"""

	# file_name = path.replace('\\','/').split('/')[-1]

	try:
		shutil.rmtree(path)
		return path+' deleted!'
	except Exception as e:
		print(e)

def formatPath(path):
	""" Formath the path according with the operational system. If the script is running in windows, the path should have the follow format
		'C:\\path\\to\\file' and in linux '/path/to/file'
	:param path: Path 
	:return:
	"""

	if os.name == 'nt':
		return path.replace('/','\\')
	else:
		return path.replace('\\','/')




if __name__ == '__main__':
	create_tmp_pwd()
	# file='/Users/scher/git/wx_downloader/wx_chuva_vazao_downloader/cv_downloader.py'
	# to_path = '/Users/scher/git/wx_downloader/wx_chuva_vazao_downloader'
	# create_file_copy(file,to_path,'copy.py')

	# move_file(to_path+'/copy.py', to_path+'/libs/tmp')

	# delete_file(to_path+'/libs/tmp/copy.py')
	# delete_file(to_path+'/tmp/')