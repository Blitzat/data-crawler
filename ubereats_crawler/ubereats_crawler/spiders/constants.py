## URL
URL_ROOT = "https://www.ubereats.com"

## XPath

# find all categories under main page
XPATH_CATEGORIES = \
  "//*[@id='main-content']/div[5]/div/div/div/div/div/div[2]//@href"

# find all restaurant under a specific category
XPATH_RESTAURANTS = "//*[@id='main-content']/div[4]/div[1]//@href"

# find script that includes all url
XPATH_UUID_SCRIPT = "//*[@id='__REDUX_STATE__']/text()"