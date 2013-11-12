from ab_scrape_urls.rooms_scraper import DataScraper
from database.db import *

class run:
    def __init__(self,url):
        scraper=DataScraper(url)
        l=listing()
        c=calender()
        loc=location()
        scraper.set_listing(l)
        scraper.set_calender(c)
        scraper.set_location(loc)
        scraper.start_crawl()


#run("https://www.airbnb.com/s/San-Francisco--CA?source=bb")
for i in range(6,16):
    run("https://www.airbnb.com/s/San-Francisco--CA?guests=%s&source=bb"%str(i))


