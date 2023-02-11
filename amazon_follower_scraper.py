from selenium import webdriver
from selenium.webdriver.common.by import By
import re
import time
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
import json
import chromedriver_autoinstaller
import time
from selenium.common.exceptions import StaleElementReferenceException
import json
import psycopg2

options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument("--window-size=1366,768")

# get login data
login = {}
with open('login_info.json', 'r') as f:
    login = json.load(f)

db_con = {}
with open('db_con.json', 'r') as f:
    db_con = json.load(f)
print('connecting to db...')
conn = psycopg2.connect(database="postgres",
                        host=db_con['host'],
                        user=db_con['user'],
                        password=db_con['password'],
                        port=db_con['port'])
cur = conn.cursor()
# Fetch usernames in db
cur.execute("SELECT * FROM public.\"Usernames\";")

rows = cur.fetchall()
usernames_db = {}
for row in rows:
    usernames_db[row[0]]=1
# needed to install chrome driver


chromedriver_autoinstaller.install() 


# open it, go to a website, and get results
wd = webdriver.Chrome(options=options)


print('done setup')

print('logging in')

wd.get('https://www.pinterest.ca/login')
# wd.save_screenshot('./test.png')
email = wd.find_element(By.ID, 'email')
email.send_keys(login['pinterest']['email'])
password = wd.find_element(By.ID, 'password')
password.send_keys(login['pinterest']['password'])

login_button = wd.find_elements(By.XPATH, "//*[contains(text(), 'Log in')]")[1]
# Image(login_button.screenshot_as_png)
login_button.click()


# go to amazon
time.sleep(2)
wd.get('https://www.pinterest.ca/amazon/')
time.sleep(4)
# wd.save_screenshot('./test.png')  
# click followersc 
elem = wd.find_element(By.XPATH, '//*[@data-test-id="profile-followers-count"]')
elem.click()
# easiest just to wait for loading
time.sleep(2)

usernames = {}
target = 5000

# scroll
element = wd.find_element(By.XPATH, '//*[@data-test-id="profile-followers-feed"]')

while len(usernames) < target:
    wd.execute_script('arguments[0].scrollBy(0, 400);', element)
    time.sleep(0.5)
    elems = wd.find_elements(By.XPATH, '//*[@data-test-id="user-rep"]')
    try:
        for elem in elems:
            uname = str(elem.find_element(By.CSS_SELECTOR,"a[href]").get_attribute('href'))[25:-1]
            if not uname in usernames and not uname in usernames_db:
                usernames[uname] = 1
    except StaleElementReferenceException:
        pass

for username in usernames:
    cur.execute(f'INSERT INTO public."Usernames" ("Username", "Scraped", "InUse", "User") VALUES (\'{username}\', false, false, NULL);')
conn.commit()


print('closing browser')
wd.close()
print('closing db connection')
cur.close()
conn.close()
print('saving data')

# Serializing json
json_object = json.dumps(usernames, indent=4)
 
# Writing to sample.json
with open("./usernames.json", "w") as outfile:
    outfile.write(json_object)


