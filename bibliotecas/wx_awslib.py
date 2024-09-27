# pip install awscli
# pip install boto3

# To create a new configuration:

#    $ aws configure
#    AWS Access Key ID [None]: accesskey
#    AWS Secret Access Key [None]: secretkey
#    Default region name [None]: us-west-2
#    Default output format [None]:

# To update just the region name:

#    $ aws configure
#    AWS Access Key ID [****]:
#    AWS Secret Access Key [****]:
#    Default region name [us-west-1]: us-west-2
#    Default output format [None]:

# WX_api


import boto3
from botocore.exceptions import ClientError
# from boto3.session import Session
import zipfile
from time import sleep
from paramiko import SSHClient
import paramiko
import os
import pdb


try:
	from libs.wx_log import logging
except:
	from wx_log import logging
	

class WxAws:

	def __init__(self):
		self.region_name = 'us-east-1'
		self.bucketName = 'wx-prospec'

	def downloadFile(self, fileName):
		s3 = boto3.client('s3')
		objectName = fileName
		fileName = fileName.split('/')[-1]
		s3.download_file(self.bucketName, objectName, fileName)

	def uploadFile(self, fileName, pathAWS='./'):
		s3 = boto3.client('s3', region_name=self.region_name)
		with open(fileName, "rb") as f:
			objectName = fileName
			fileName = fileName.split('/')[-1]
			print(pathAWS+fileName)
			s3.upload_fileobj(f, self.bucketName, pathAWS+fileName)

	def uploadFolder(self, bucket_name, folder_path):
		pdb.set_trace()
		try:
			s3 = boto3.resource('s3', region_name=self.region_name)
			bucket = s3.Bucket(bucket_name)
			for path, subdirs, files in os.walk(folder_path):
				path = path.replace("\\","/")
				directory_name = path.replace(folder_path,"")
				for file in files:
					bucket.upload_file(os.path.join(path, file), directory_name+'/'+file)

		except Exception as e:
			print(e)
			quit()


	def getAllFileNames(self):
		s3 = boto3.client('s3')
		paginator = s3.get_paginator('list_objects')
		page_iterator = paginator.paginate(Bucket=self.bucketName)

		lst1 = []
		for page in page_iterator:
			for i in page['Contents']:
				if '.' not in i['Key'] and 'instaladores/' not in i['Key']:
					lst1.append(i['Key'])
		return(lst1)

	def getFileNamesInFolder(self, prefix):
		s3 = boto3.client('s3')
		keys = s3.list_objects(Bucket=self.bucketName, Prefix=prefix)
		data={'zip':[], 'info':[]}
		for key in keys['Contents']:
			if '.' not in key['Key']:
				data['info'].append(key['Key'])
			if '.zip' in key['Key'] and '/NW' not in key['Key']:
				data['zip'].append(key['Key'])
		return data

	def launchEc2Instance(self, action, instance_id):

		ec2 = boto3.client('ec2')
		response = ec2.describe_instances()

		if action == 'ON':
			# Do a dryrun first to verify permissions
			try:
				ec2.start_instances(InstanceIds=[instance_id], DryRun=True)
			except ClientError as e:
				if 'DryRunOperation' not in str(e):
					raise

			# Dry run succeeded, run start_instances without dryrun
			try:
				response = ec2.start_instances(InstanceIds=[instance_id], DryRun=False)
			except ClientError as e:
				print(e)
		else:
			# Do a dryrun first to verify permissions
			try:
				ec2.stop_instances(InstanceIds=[instance_id], DryRun=True)
			except ClientError as e:
				if 'DryRunOperation' not in str(e):
					raise

			# Dry run succeeded, call stop_instances without dryrun
			try:
				response = ec2.stop_instances(InstanceIds=[instance_id], DryRun=False)
			except ClientError as e:
				print(e)

	def getInstanceStatus(self, instance_id, status='running', max_loop = 10):
		# status pode ser 'stopped', 'terminated', 'running'

		session = boto3.Session()
		ec2 = session.resource('ec2')

		loop = 0
		# maximo de 10 checks 
		while loop < max_loop:
			# Filtra todas as estancias com status passado no parametro
			instances = ec2.instances.filter(
				Filters=[{'Name': 'instance-state-name', 'Values': [status]}])

			# Verifica todas as intancias filtradas
			for instance in instances:
				if instance.id == instance_id:
					# print(str(instance.id) + ' com status ' + status)
					return True
			if max_loop > 1:
				print("Tentativa " + str(loop+1))
			sleep(5)
			loop += 1
		# print("Máximo de interacoes foi atingido e a instancia (id:"+str(instance_id)+") não atingiu o status esperado("+status+")")
		return False

class SSH:
    def __init__(self, server, username, password):
        self.ssh = SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname=server ,username=username ,password=password, allow_agent=False, look_for_keys=False, timeout=30)

    def exec_cmd(self,cmd):
        stdin,stdout,stderr = self.ssh.exec_command(cmd)

        if stderr.channel.recv_exit_status() != 0:
            # print(str(stderr.read().decode('utf8', 'ignore')))
            # logging.info(str(stderr.read().decode('utf8', 'ignore')))
            return str(stderr.read().decode('utf8', 'ignore'))

        else:
            # print(stdout.read().decode('utf8', 'ignore'))
            # logging.info(str(stdout.read().decode('utf8', 'ignore')))
            return str(stdout.read().decode('utf8', 'ignore'))

    def close_conection(self):
    	self.ssh.close()


if __name__ == '__main__':
	# fileName = 'estudo_0679/compila_ea.csv'
	# awxServe.downloadFile(fileName)
	# awxServe = WxAws()
	# awxServe.getAllFileNames()
	# awxServe.getInFolder('estudo_0679/')
	# awxServe.uploadFile('compila_ea.csv')
	

	# awxServe = WxAws()
	# awxServe.launchEc2Instance(action='ON', instance_id= 'i-0c8b51044f4f0580e')
	# awxServe.getInstanceStatus(instance_id= 'i-0c8b51044f4f0580e', status = 'running',max_loop = 1)
	# awxServe.launchEc2Instance(action='OFF', instance_id= 'i-0c8b51044f4f0580e')
	# awxServe.getInstanceStatus(instance_id= 'i-0c8b51044f4f0580e', status = 'stopped')
	# quit()
	# ssh.exec_cmd("cd C:/Users/Administrator/wx_workspace/wx_smap && dir")

	# /usr/local/aws-cli/v2/2.0.4/bin/aws s3 sync 20200317 s3://wx-chuva-vazao/Chuva-vazão
	
	# awxServe = WxAws()
	# bucket_name = 'wx-chuva-vazao'
	# folder_path = 'C:/Users/thiag/WX Energy Dropbox/WX - Middle/NovoSMAP/20200317/'
	# awxServe.uploadFolder(bucket_name, folder_path)
	pass
