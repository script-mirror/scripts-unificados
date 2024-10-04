
import os 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

__USER_SINTEGRE = os.getenv('USER_SINTEGRE') 
__PASSWORD_SINTEGRE = os.getenv('PASSWORD_SINTEGRE')

nomeUser='arthur rafael franco de moraes'


def check_login_ONS(driver):
    try:
        nome = driver.find_element(By.CLASS_NAME, "user-name").text

        nome = nome.strip().lower()
        if nome == nomeUser:
            print('Login feito com sucesso!')
            return True
    except:
        return False


def login_ons(driver):

    driver.get("https://sintegre.ons.org.br/sites/9")

    WebDriverWait(driver, 30).until(
    EC.visibility_of_element_located((By.ID, "corpo"))
    )
    tentativas = 10
    try:
        user = driver.execute_script('document.getElementsByClassName("user-name")[0].textContent')
    except:

        if not set_user(driver):
            if not set_password(driver):
                pass
        else:
            set_password(driver)

    while tentativas:
        if check_login_ONS(driver):return True
        tentativas-=1



def set_user(driver):

    WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.ID, "username")))
    try:
        user = driver.find_element(By.ID, "username")
        user.clear()
        user.send_keys(__USER_SINTEGRE)
        driver.find_element(By.NAME, "submit.IdentificarUsuario").click()
        return True

    except:
        return False
    

def set_password(driver):

    WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.ID, "password")))
    try:
        password = driver.find_element(By.ID, "password")
        password.clear()
        password.send_keys(__PASSWORD_SINTEGRE)
        driver.find_element(By.NAME, "submit.Signin").click()
        return True

    except:
        return False