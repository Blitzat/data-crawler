## URL
URL_ROOT = "https://www.ubereats.com"

# API to get store uuids by city and category
URL_GET_SEO_FEED = "https://www.ubereats.com/api/getSeoFeedV1"

# API to get information of each store including all menus
URL_GET_STORE_INFO = "https://www.ubereats.com/api/getStoreV1"

## XPath

# find all categories under main page
XPATH_CATEGORIES = "//*[@id='main-content']/div[4]//@href"

# find all restaurant under a specific category
XPATH_RESTAURANTS = "//*[@id='main-content']/div[4]/div[1]//@href"

# find script that includes all url
XPATH_UUID_SCRIPT = "//*[@id='__REDUX_STATE__']/text()"