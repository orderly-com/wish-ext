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
        return ['member_transaction_no']

    @staticmethod
    def get_headers_array():
        return ['id', 'member_transaction_no', 'sod_id', 'number', 'sale_price', 'retail_price', 'so_amount', 'so_discount_amt', 'member_transaction_type', 'code', 'name', 'act_id', 'company_code', 'created_at', 'updated_at']

    def process_dataset(self):
        pass


@wish_ext.datasource('威許會員格式')
class ClientFileImporter(FileImporter):
    data_importer = ClientImporter

    @staticmethod
    def get_required_headers():
        return ['no']

    @staticmethod
    def get_headers_array():
        return ['id', 'no', 'sex', 'birthday', 'registered_at', 'first_transaction_at', 'end_transaction_at', 'type', 'activity_type', 'postal_code', 'city', 'district', 'last_updated_at', 'company_code', 'created_at', 'updated_at']

    def process_dataset(self):
        pass


@wish_ext.datasource('威許會員格式')
class ClientFileImporter(FileImporter):
    data_importer = ClientImporter

    @staticmethod
    def get_required_headers():
        return ['no']

    @staticmethod
    def get_headers_array():
        return ['id', 'no', 'sex', 'birthday', 'registered_at', 'first_transaction_at', 'end_transaction_at', 'type', 'activity_type', 'postal_code', 'city', 'district', 'last_updated_at', 'company_code', 'created_at', 'updated_at']

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
