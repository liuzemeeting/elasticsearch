from scrapy.exporters import JsonLinesItemExporter
class chongxie(JsonLinesItemExporter):
    def __init__(self,file,**kwages):
        print("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
        super(chongxie,self).__init__(file,ensure_ascii=None)