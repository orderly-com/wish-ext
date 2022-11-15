from importly.importers import FileImporter

from orderly_core.team.importers import ClientImporter
from ..retail_importly.importers import OrderImporter
from ..wish_importly.importers import (
    LevelImporter, EventImporter, PointLogImporter, LevelLogImporter, EventLogImporter
)

from ..extension import wish_ext



@wish_ext.datasource('威許訂單格式')
class OrderFileImporter(FileImporter):
    data_importer = OrderImporter

    @staticmethod
    def get_required_headers():
        return ['會員編號']

    @staticmethod
    def get_headers_array():
        return ['品牌名稱', '交易時間', '交易時段', '門市id', '門市名稱', '訂單編號', '發票金額', '交易型態(一般/退貨)', '會員編號', '發票號碼', '是否首購', '是否會員', '付費方式', 'member_card_id', '訂單明細流水號', '購買數量', '產品原價', '產品唯一碼', '產品名稱', '折扣後單價', '活動代碼', '品項小計', '折扣金額']

    def process_dataset(self):
        pass


@wish_ext.datasource('威許會員格式')
class ClientFileImporter(FileImporter):
    data_importer = ClientImporter

    @staticmethod
    def get_required_headers():
        return ['會員編號']

    @staticmethod
    def get_headers_array():
        return ['會員編號', '會員卡編號', '姓名', '性別', '電話', '信箱', '生日', '會員建立時間', '會員首購時間', '會員最後購買時間', 'NESL', 'R', 'F', 'M', '會員所屬縣市郵遁區號', '會員所屬縣市', '會員所屬鄉鎮區', '點數/儲值金/優惠券...', '剩餘數值', '最近一次到期時間', '最近一次到期數值', '修改時間(預留)', '等級', '等級代碼', '等級即將到期', 'Facebook 綁定', 'LINE 綁定', 'Google 綁定', 'Apple 綁定']

    def process_dataset(self):
        pass


@wish_ext.datasource('威許等級格式')
class LevelFileImporter(FileImporter):
    data_importer = LevelImporter

    @staticmethod
    def get_required_headers():
        return ['對應代碼', '等級名稱']

    @staticmethod
    def get_headers_array():
        return ['等級 id', '對應代碼', '等級名稱', '位階（數字越大，等級越高）']

    def process_dataset(self):
        pass


@wish_ext.datasource('威許等級記錄格式')
class LevelLogFileImporter(FileImporter):
    data_importer = LevelLogImporter

    @staticmethod
    def get_required_headers():
        return ['會員編號', '原始等級', '後來等級', '等級起始時間']

    @staticmethod
    def get_headers_array():
        return ['id', '會員編號', '原始等級', '後來等級', '等級起始時間', '等級到期時間', '來源模組', '建立時間']

    def process_dataset(self):
        pass


@wish_ext.datasource('威許點數記錄格式')
class PointLogFileImporter(FileImporter):
    data_importer = PointLogImporter

    @staticmethod
    def get_required_headers():
        return ['會員編號', '幣別名稱', '點數數量']

    @staticmethod
    def get_headers_array():
        return ['id', '會員編號', '幣別名稱', '點數數量', '點數到期日', '來源模組', '建立時間', '品牌代碼', '品牌名稱', '門市代碼', '門市名稱']

    def process_dataset(self):
        pass


@wish_ext.datasource('威許活動格式')
class EventFileImporter(FileImporter):
    data_importer = EventImporter

    @staticmethod
    def get_required_headers():
        return ['活動id', '票券名稱', '免費/點數/兌換碼']

    @staticmethod
    def get_headers_array():
        return ['品牌名稱', '活動id', '兌換票劵/領取點數', '活動名稱', '票券類型（共七種票券類型）', '票券名稱', '簡易描述', '免費/點數/兌換碼', '活動狀態', '活動額度', '活動開始時間', '活動結束時間']

    def process_dataset(self):
        pass


@wish_ext.datasource('威許活動記錄格式')
class EventLogFileImporter(FileImporter):
    data_importer = EventLogImporter

    @staticmethod
    def get_required_headers():
        return ['活動ID', '動作 / 兌換']

    @staticmethod
    def get_headers_array():
        return ['記錄ID', '線上 / 線下', '活動名稱', '活動ID', '日期', '動作 / 兌換', '數值', '單位', '會員編號', '訂單編號', '訂單明細項次編號']

    def process_dataset(self):
        pass
