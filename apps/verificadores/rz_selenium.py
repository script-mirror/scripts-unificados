import os
import time
import psutil
from selenium import webdriver
import undetected_chromedriver as uc


from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))


file_path = os.path.dirname(os.path.abspath(__file__))
path_local_tmp = os.path.join(file_path,'tmp')

PATH_CHROME_PROFILE = "C:/Users/CS399274/AppData/Local/Google/Chrome/User Data/Default"
# PATH_CHROME_PROFILE = "/home/admin/.config/google-chrome/Default"
PATH_TMP_DOWNLOAD = os.path.join(path_local_tmp,str(time.time()))

PATH_FIREFOX_BINARY = "/usr/bin/firefox"
# PATH_FIREFOX_BINARY = "C:/Users/CS399274/AppData/Local/Mozilla Firefox/firefox.exe"
PATH_FIREFOX_DRIVER = os.path.join(file_path,"geckodriver")


def killpid(pid):	
	try:
		p = psutil.Process(pid)
	except:
		return 

	children = p.children(recursive=True)
	while len(children) > 1:
		os.system(f'kill -9 {children[0].pid}')
		children = p.children(recursive=True)
            
	os.system(f"kill -9 {p.pid}")



def abrir_firefox(firefox_binary=None,driver_path=None):
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options
    from selenium.webdriver.firefox.service import Service

    if not firefox_binary:
        firefox_binary = PATH_FIREFOX_BINARY

    if not driver_path:
        driver_path = PATH_FIREFOX_DRIVER

    options = Options()
    options.binary_location = firefox_binary
    # options.add_argument('--headless')
    # options.add_argument('--disable-dev-shm-usage')

    # options.profile= PATH_PROFILE
    serv = Service(driver_path)
    driver = webdriver.Firefox(options=options,service=serv)

    return driver


def abrir_undetected_chrome( versao_chrome = 94,path_tmp_download=PATH_TMP_DOWNLOAD):

    options = uc.ChromeOptions()
    # options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    # options.add_argument('--single-process')
    options.add_argument('--disable-dev-shm-usage')
    # options.add_argument(f"user-data-dir={PATH_CHROME_PROFILE}")
    options.add_experimental_option("prefs", {
        "download.default_directory": path_tmp_download,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
        })

    driver = uc.Chrome(options=options, version_main=versao_chrome)

    return driver

def is_file_downloaded(filename, timeout=120):
    end_time = time.time() + timeout
    while not os.path.exists(filename):
        time.sleep(1)
        if time.time() > end_time:
            print("File not found within time")
            return False
    print("File found")
    return True

def kill_selenium_pid(selenium):
    try:
        pid = selenium.service.process.pid
        proc = psutil.Process(pid)
        pid_child = proc.children(recursive=True)[0].pid
        killpid(pid_child)
    except:
        return