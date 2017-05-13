from scrapy.spiders import Spider 
from scrapy import Item

#新闻 体育  娱乐 时尚 教育 城市 旅游 科技  财经  博客

class SinaSpider(Spider): 
    name = "sina" 
    allowed_domains = ["thepaper.cn"] 
    start_urls = [
        'http://www.thepaper.cn/'
    ] 
 
    def parse(self, response): 
        print(response.url)
        i = Item()
        return i