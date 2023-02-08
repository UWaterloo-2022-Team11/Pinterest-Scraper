# start selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
import re
import time
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
import json
import chromedriver_autoinstaller
import time
# from selenium.common.exceptions import StaleElementReferenceException
import json
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.keys import Keys
# from selenium.common.exceptions import StaleElementReferenceException

import signal
import psycopg2
import socket
import logging
import argparse
import requests
import os
import time


pid = os.getppid()
localhost = 'http://localhost:5000'
# register scraper
requests.get(f'{localhost}/register/{pid}')


parser = argparse.ArgumentParser()
parser.add_argument('--limit', type=int, dest='limit', default=400,
                    help='an integer for the number of users to be scraped')

args = parser.parse_args()

logging.basicConfig(filename=f'{pid}-scraper.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

device_name = socket.gethostname()
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument("--window-size=1366,768")

# get login data
# You will need to manually mount gdrive
login = {}
with open('login_info.json', 'r') as f:
    login = json.load(f)

db_con = {}
with open('db_con.json', 'r') as f:
    db_con = json.load(f)

print(db_con['host'])
# needed to install chrome driver


chromedriver_autoinstaller.install() 
#'c:\\Users\\Eric\\anaconda3\\lib\\site-packages\\chromedriver_autoinstaller\\105\\chromedriver.exe'


# open it, go to a website, and get results
wd = webdriver.Chrome(options=options)


print('done setup')

print('logging in')

wd.get('https://www.pinterest.ca/login')
email = wd.find_element(By.ID, 'email')
email.send_keys(login['pinterest']['email'])
password = wd.find_element(By.ID, 'password')
password.send_keys(login['pinterest']['password'])

login_button = wd.find_elements(By.XPATH, "//*[contains(text(), 'Log in')]")[1]
login_button.click()


print('connecting to db...')
conn = psycopg2.connect(database="postgres",
                        host=db_con['host'],
                        user=db_con['user'],
                        password=db_con['password'],
                        port=db_con['port'])
cur = conn.cursor()

def shutdown():
    requests.get(f'{localhost}/finished/{pid}')
    print('shutting down browser')
    wd.close()
    print('closing db connection')
    cur.close()
    conn.close()
    exit(0)

def keyboardInterruptHandler(signal, frame):
    shutdown()

# actually scrape a user
def get_pins():
    return wd.find_elements(By.XPATH, '//*[@data-test-id="pin"]')

def get_data(element):
    
    html = element.get_attribute('innerHTML')
    # find pin id in html with a regex
    # just look at first item, shouldnt be more then one
    # then slice of pin/ part and convert to int
    pinid = int(re.findall(r'pin/\d+', html)[0][4:])
    img = element.find_element(By.TAG_NAME, 'img').get_attribute('src')
    # hover over pin
    hover = ActionChains(wd).move_to_element(element)
    hover.perform()
    raw_html = element.get_attribute('innerHTML')
    
    linkdiv = element.find_elements(By.XPATH, '//*[@data-test-id="pinrep-source-link"]')
    data = ''
    if len(linkdiv):
        link = linkdiv[0].find_elements(By.TAG_NAME, 'a')[0]
        data = link.get_attribute('href')
    return {
        'id': pinid,
        'img': img,
        'link': data,
        'raw': raw_html
    }
    
    
def scrape_user(username, max_elapse=600):
    start = time.time()
    # wd.get(f'https://www.pinterest.ca/{username}/')
    # WebDriverWait(wd, 10).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@data-test-id="pwt-grid-item"]'))
    # )
    pin_count = -1
#     try:
#         pins = wd.find_elements(By.XPATH, '//*[@data-test-id="pwt-grid-item"]')
#         header = pins[0].find_element(By.XPATH, "//h2[@title='All Pins']")
#         parent = header.find_element(By.XPATH, '..').find_element(By.XPATH, '..')
#         child = parent.find_element(By.CSS_SELECTOR, 'div:nth-of-type(2)')
#         final = child.get_attribute('innerHTML')
#         pin_count = int(re.findall(r'\b\d+[,\d]*\b Pins', final)[0][:-4].replace(',', ''))
#     except Exception as e:
#         # Log the exception type and message
#         print("Exception type:", type(e))
#         print("Exception message:", e)
        
    
    link = f'https://www.pinterest.ca/{username}/pins'
    wd.get(link)
    

    body = wd.find_element(By.TAG_NAME, 'body')
    
    wait = WebDriverWait(wd, 10)
    wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@data-test-id="pin"]')))
#     wd.save_screenshot('./test.png')
    pins = {}
    count = 0
    while 1:
        elems = get_pins()
        initial_pin_count = len(elems)
        added = False
        for elem in elems:
            try:
                pin = get_data(elem)
                if not pin['id'] in pins:
                    added = True
                    pins[pin['id']] = pin
                    count += 1
            except Exception as e:
                try:
                    print(pin.get_attribute('innerHTML'))
                except:
                    pass
        if pin_count > 0 and count >= pin_count-1:
            print('pin cnt')
            break
#         if not added:
#             break
#         body.send_keys(Keys.PAGE_DOWN)

        wd.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # chatgpt uses this to scroll
        # window.scrollTo(0, document.body.scrollHeight);
#         time.sleep(2) # give time for images to load?
        
        
        # test chat gpt code
#         try:
#             wait.until(lambda driver: len(pins) > initial_pin_count)
#         except Exception as e:
#             print('excepotioin done')
#             break
#         pins = wd.find_elements(By.XPATH, '//*[@data-test-id="pin"]')
        
        # detect if at bottom of page
        time.sleep(1.5)
        at_bottom = wd.execute_script('return (window.innerHeight + window.scrollY) >= document.body.offsetHeight-10')
        if at_bottom:
            print('at bottom')
            break

        if time.time()-start>max_elapse:
            print('max time')
            break
#         wd.save_screenshot('./test.png')
    return pins

signal.signal(signal.SIGINT, keyboardInterruptHandler)
logging.log(0, "Starting log")
while True:
    # check if scraper should continue
    if not requests.get(f'{localhost}/active/{pid}').json()['active']:
        shutdown()

    # get usernames we want to scrape
    cur.execute("SELECT * FROM public.\"Usernames\" WHERE \"InUse\"=False AND  \"Scraped\"=False LIMIT 4;")
    
    # Fetch the results
    rows = cur.fetchall()
    usernames = []
    for row in rows:
        usernames.append(row[0])

    for username in usernames:
        cur.execute(f"UPDATE public.\"Usernames\" SET \"InUse\" = true WHERE \"Username\" = '{username}';")
    conn.commit()

    failed_users = '' 
    user_count = 0
    for user in usernames:
        user_count += 1
        try:
            pins = scrape_user(user)
            # import pickle 
            # filehandler = open(f'{user}-pickle0.pkl', 'wb') 
            # pickle.dump(pins, filehandler)
            # filehandler.close()

            username = device_name
            logging.log(0, f'saving: {len(pins)} for user: {user}')
            for pid in pins:
                pin = pins[pid]
                image_url = pin['img']
                image_url = psycopg2.extensions.adapt(image_url).getquoted()
                pinid = pin['id']
                link_url = pin['link']
                link_url = psycopg2.extensions.adapt(link_url).getquoted()
                if len(link_url) > 4000:
                    logging.warning(f'link failed on user: {user} link: {pin["link"]}')
                    link_url = ''
                cur.execute(
                    "INSERT INTO public.\"PinData\" (pinid, img, link, username) VALUES (%s, %s, %s, %s);", (pid, image_url, link_url, user)
                )
            cur.execute(f"UPDATE public.\"Usernames\" SET \"Scraped\" = true WHERE \"Username\" = '{device_name}';")
            conn.commit()

            requests.get(f'{localhost}/scraped_user/{pid}/{user}')
        except Exception as e:
            print(f'failed on user: {user}')
            print(e)
            wd.save_screenshot(f'./{user}-error.png')
            logging.error(e)
            logging.warning(f'failed on user: {user}')
            requests.get(f'{localhost}/failed_user/{pid}/{user}')
        if args.limit and user_count > args.limit:
            keyboardInterruptHandler('', '')
