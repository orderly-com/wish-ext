from importly.importers import FileImporter

from orderly_core.team.importers import ClientImporter
from ..retail_importly.importers import OrderImporter

from ..extension import retail_ext



@retail_ext.datasource('威許訂單格式')
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


@retail_ext.datasource('威許會員格式')
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
