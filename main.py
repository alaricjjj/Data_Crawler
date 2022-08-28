from North_Fund_Crawler.BX_Crawler import NorthFundCrawler

path_params = {
    '北向资金': '/Volumes/Alaric_Disk/Quant_DB/北向资金'
}


class MainCrawler:

    def __init__(self, save_path):
        self.north_fund_save_path = save_path['北向资金']

    def get_north_fund_data(self):
        instance = NorthFundCrawler()
        instance.main_crawler(save_path=self.north_fund_save_path)

    def data_crawler(self, data_target, update=False):
        if data_target == '北向资金':
            if update:
                message = '数据更新程序有待开发'
                print(message)
            else:
                message = '开始下载数据'
                print(message)
                self.get_north_fund_data()


if __name__ == "__main__":
    main_instance = MainCrawler(save_path=path_params)
    main_instance.data_crawler(data_target='北向资金')
