from scrapy.spiders import Spider 
from scrapy import Item

#���� ����  ���� ʱ�� ���� ���� ���� �Ƽ�  �ƾ�  ����

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