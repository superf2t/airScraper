import eventlet
import random
from scrapy.selector import HtmlXPathSelector
from eventlet.green import urllib2,urllib
from lxml.html import fromstring
from db_item import item
import datetime
import calendar
import re

import json
eventlet.monkey_patch()

def add_months(sourcedate,months):
   month = sourcedate.month - 1 + months
   year = sourcedate.year + month / 12
   month = month % 12 + 1
   day = min(sourcedate.day,calendar.monthrange(year,month)[1])
   return datetime.date(year,month,day)

class DataScraper:
    """Scraper for San Fracisco, but it can be used for any city
    city_url="https://www.airbnb.com/s/San-Francisco--CA?source=bb"""

    def __init__(self,city_url):
        self.request_counter=1
        self.scrape_started_date=datetime.datetime.now()
        self.pool=eventlet.GreenPool(10)
        self.requests=eventlet.Queue()
        self.seen=set()
        self.requests.put((self.parse_first,city_url,{}))
        self.domain='airbnb.com'
        self.domain_url='https://www.airbnb.com'
        self.search_other_pages="/search/search_results?page=%s"
        self.search_date_url="https://www.airbnb.com/rooms/calendar_tab_inner2/%s?cal_month=%s&cal_year=%s&currency=USD"
        self.scrape_finished=False

    def set_listing(self,value):
        self.listing=value

    def set_calender(self, value):
        self.calender=value

    def set_location(self, value):
        self.location=value

    def fetch(self,url,full_parse=True,cookie=None,token=None):
        if self.request_counter%2:
            eventlet.sleep(random.randint(6,10))
        self.request_counter+=1
        print "fetching:%s"%url
        req=urllib2.Request(url)
        if cookie!=None:
            req.headers['Accept']='*/*'
        if cookie!=None:
            req.headers['Cookie']=cookie
            req.headers['X-Requested-With']='XMLHttpRequest'
        req.headers['User-Agent']='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36'
        if token!=None:
            req.headers['X-CSRF-Token']=token
        res=urllib2.urlopen(req)
        html=res.read()
        pLen=int(0.50*len(html))
        if html[0:pLen] in set(self.partial_scraped_data.values()) and "/rooms/" not in url:
            self.scrape_finished=True
            return (res,"")
        self.partial_scraped_data[url]=html[0:pLen]
        if full_parse:
            return (res,HtmlXPathSelector(text=html))
        else:
            return (res,html)

    def parse_first(self,url,data={}):
        """Fetch url for scraping and parsing the data with a right function"""
        xs=self.fetch(url)[1]
        for link in xs.select("//div[@class='listing-outer']//a[@class='target-details']/@href").extract():
            self.requests.put((self.parse_room,self.domain_url+link,{}))
        self.full_city=xs.select("//input[@name='location']/@value").extract()[0]
        query=urllib.splitquery(url)
        if len(query)>1:
            query=query[1].replace("&source=bb","")
        else:
            query=""
        self.requests.put((self.parse_rest,self.domain_url+self.search_other_pages%"2&"+urllib.urlencode({"location":self.full_city})+"&"+query,{'current_page':'2','query':query}))

    def parse_rest(self,url,data={}):
        html_json=self.fetch(url,False)[1]
        next_page=str(int(data['current_page'])+1)
        self.requests.put((self.parse_rest,self.domain_url+self.search_other_pages%next_page+"&"+urllib.urlencode({'location':self.full_city})+"&"+data['query'],{'current_page':next_page,"query":data['query']}))
        try:
            html=json.loads(html_json)
            xs=HtmlXPathSelector(text=html['results'])
            for link in xs.select("//div[@class='listing-outer']//a[@class='target-details']/@href").extract():
                self.requests.put((self.parse_room,self.domain_url+link,{}))


        except:
            #ADD LOGGER
            self.scrape_finished=True

    def parse_room(self,url,data={}):
        res,xs=self.fetch(url)
        i=item(self.listing)
        try:
            i.description="".join(xs.select("//div[@id='description_text_wrapper']//p//text()").extract())
        except:
            pass

        i.airbnb_id=url.split("/")[4].split("?")[0]
        try:
            i.bathroom=str(int(float("".join(xs.select("//*[contains(text(),'Bathrooms:')]/..//following-sibling::td/text()").extract()))*100))
        except:
            i.bathroom='0'
        i.date_added='NOW()'
        i.link=url
        i.location_id='0'
        i.map_coordinates=",".join(xs.select("//meta[contains(@property,'latitude') or contains(@property,'longitude')]/@content").extract())
        i.title="".join(xs.select("//h1[@itemprop='name']/@title").extract())
        try:
            i.bedroom=str(int(float("".join(xs.select("//*[contains(text(),'Bedrooms:')]/..//following-sibling::td/text()").extract()))*100))
        except:
            i.bedroom='0'
        name=xs.select("//span[@id='display-address']/@data-location").extract()[0]
        self.listing.search_and_insert_with_location(*(i.values()+[name]))
        for ii in range(0,7):
            scrape_date_added=add_months(self.scrape_started_date,ii)
            #"https://www.airbnb.com/rooms/calendar_tab_inner2/364397?cal_month=11&cal_year=2013&currency=USD"
            url_new=self.search_date_url%(i.airbnb_id,scrape_date_added.month,scrape_date_added.year)
            auth_token=(xs.select("//form//input[@name='authenticity_token']/@value").extract())[0]
            self.requests.put((self.parse_dates,url_new,{'airbnb_id':i.airbnb_id,'token':auth_token,'cookie':res.headers['Set-Cookie'],'month':scrape_date_added.month,'year':scrape_date_added.year}))

    def parse_dates(self,url,data={}):
        res,xs=self.fetch(url,True,data['cookie'],data['token'])
        i=item(self.calender)
        try:
            i.listing_id=str(self.listing.fetch(airbnb_id=data['airbnb_id'])[0][0])
        except:
            #TODO ADD LOGGER
            return
        in_month=False
        for td in xs.select("//td[contains(@id,'day')]"):
            if td.select("./span/text()").extract()[0]=='1':
                if in_month==False:
                    in_month=True
                else:
                    in_month=False
            if in_month:
                i.price_date="%s-%s-%s"%(data['year'],data['month'],td.select("./span/text()").extract()[0])
                clas=td.select("./@class").extract()[0]
                if "unavailable" in clas:
                    i.is_booked='Y'
                    i.price='0'
                    self.calender.insert_without_commit(*i.values())
                elif " available" in clas:
                    i.is_booked='N'
                    i.price=td.select("./div/text()").extract()[0].replace("$","").strip()
                    self.calender.insert_without_commit(*i.values())
        self.calender.commit_only()






    def start_crawl(self):
        try:
            self.partial_scraped_data={}
            seen=[]
            while True:

                while not self.requests.empty():
                    request=self.requests.get()
                    if  self.domain in request[1] and request[1] not in seen:
                        seen.append(request[1])
                        self.seen.add(request[1])
                        self.pool.spawn_n(request[0],request[1],request[2])

                self.pool.waitall()
                if self.requests.empty() or self.scrape_finished:
                    break
            print len(seen)
        except:
            import ipdb;ipdb.set_trace()

