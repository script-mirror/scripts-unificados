from paramiko import SSHClient
import paramiko

class SSH:
	def __init__(self, server='newave'):
		self.ssh = SSHClient()
		self.ssh.load_system_host_keys()
		self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

		if server == 'newave':
			self.host = 'wxe.ddns.net'
			self.username = 'wxenergy'
			self.key_filename = '/home/wxenergy/.ssh/id_rsa_edson'
			self.port =  8022


		self.connect()

	def connect(self):
		self.ssh.connect(hostname=self.host, username=self.username, key_filename=self.key_filename , port=self.port)

	def reconnect(self):
		self.connect()

	def disconnect(self):
		self.ssh.close()

	def exec_cmd(self,cmd,timeout=''):
		if timeout == '':
			stdin,stdout,stderr = self.ssh.exec_command(cmd)
		else:
			stdin,stdout,stderr = self.ssh.exec_command(cmd, timeout=None, get_pty=False, bufsize=-1)

		if stderr.channel.recv_exit_status() != 0:
			return 'Error:\n{}'.format(stderr.read().decode('ascii').strip("\n"))
		else:
			# return stdout.read().decode('ascii').strip("\n")
			return stdout.read().decode('ISO-8859-1')

if __name__ == '__main__':
	ssh = SSH()
	answer = ssh.exec_cmd("cd /WX4TB/Documentos/fontes/PMO/dessem;ls")
	print(answer)