# coding: utf-8
# Developer: Deiner Zapata Silva.
# Date: 22/11/2019
# Last update: 22/11/2019
# Description: Testing selenium
# https://unipython.com/curso-selenium/
#######################################################################################

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
PATH_SELENIUM_DRIVER = 'C:\SeleniumDrivers\geckodriver\geckodriver.exe'
driver = webdriver.Firefox(executable_path=PATH_SELENIUM_DRIVER)

driver.get("https://unipython.com/los-mejores-ide-python-instalar-python-os-window-linux/")
assert "Python" in driver.title

elem = driver.find_element_by_name("s")
elem.clear()
elem.send_keys("selenium")
elem.send_keys(Keys.RETURN)
assert "No results found." not in driver.page_source

