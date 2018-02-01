import scrapy
from myfirstchong.items import MyfirstchongItem
class superspider(scrapy.Spider):
    name="park";
    start_urls=[
        "https://wenku.baidu.com/"
    ]
    def parse(self,response):
        print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        for item in response.xpath("//div/dl/dd/a"):
            title=item.xpath("text()").extract();
            targetUrl=item.xpath("@href").extract();
            oneItem=MyfirstchongItem();
            # print(title,targetUrl)
            oneItem["title"]=title;
            oneItem["targetUrl"]=targetUrl;
            print(oneItem["title"])
            
            yield scrapy.Request(url="https://wenku.baidu.com/",meta={"title":title},callback=self.parse_url)
        
    def parse_url(self,response):
        print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
        title=response.meta["title"];
        for sel2 in response.xpath("//div/dl/dd/a"):
            print("11111111111111111111111111111111111111111")

            docname=sel2.xpath("text()").extract();
            targetUrl2=sel2.xpath("@href").extract();
            oneItem=MyfirstchongItem();
            oneItem["docname"]=docname;
            oneItem["targetUrl2"]=targetUrl2;
            oneItem["image_urls"]=["https://timgsa.baidu.com/timg?image&quality=80&size=b9999_10000&sec=1511006444487&di=0f6f2711a823cf0b920d1848e8665088&imgtype=0&src=http%3A%2F%2Fimg5.duitang.com%2Fuploads%2Fitem%2F201408%2F22%2F20140822145254_He3i4.thumb.700_0.jpeg"]
            
            yield oneItem;
            # print(oneItem["docname"])
            # print(oneItem["targetUrl2"])
        
