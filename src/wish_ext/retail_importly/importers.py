import datetime
import hashlib
from dateutil import parser

from django.conf import settings
from django.db.models import Min, Sum

from importly.importers import DataImporter
from importly.formatters import (
    Formatted, format_datetime
)

from datahub.data_flows import handle_data
from datahub.models import Field, FieldGroup, ChoiceField, PrimaryField
from team.models import ClientBase
from orderly.models import Client

from ..retail.datahub import channels, DataTypeOrder
from ..retail.models import OrderBase, PurchaseBase, RetailProduct

from .formatters import format_dict, format_price, format_bool
from .models import Order, Product, OrderRow


class OrderImporter(DataImporter):

    data_type = DataTypeOrder

    class DataTransfer:
        class ClientTransfer:
            model = Client
            external_id = Formatted(str, 'client_id')

        class ProductTransfer:
            model = Product
            external_id = Formatted(str, 'product_id')
            price = Formatted(format_price, 'price')
            name = Formatted(str, 'product_name')

        class OrderTransfer:
            model = Order

            external_id = Formatted(str, 'id')
            total_price = Formatted(format_price, 'total_price')

            status = Formatted(str, 'status')
            brand_id = Formatted(str, 'brand_id')

            datetime = Formatted(format_datetime, 'datetime')

        class OrderRowTransfer:
            model = OrderRow
            refound = Formatted(format_bool, 'refound')
            sale_price = Formatted(format_price, 'sale_price')
            quantity = Formatted(format_price, 'quantity')
            total_price = Formatted(format_price, 'total_price')

    group_client = FieldGroup(key='CLIENT', name='會員')
    client_attributions = Field('會員屬性', group=group_client, is_attributions=True)

    client_id = Field('會員編號', group=group_client)

    group_order = FieldGroup(key='ORDER', name='訂單')
    brand_id = Field('品牌 ID', group=group_order)

    id = PrimaryField('訂單編號', required=True, group=group_order)


    REFOUND_CHOICES = {
        True: '是',
        False: '否'
    }
    refound = ChoiceField('是否為退貨', group=group_order, choices=REFOUND_CHOICES)

    STATUS_CHOIES = {
        PurchaseBase.STATUS_CONFIRMED: '確認',
        PurchaseBase.STATUS_ABANDONED: '取消',
        PurchaseBase.STATUS_KEEP: '處理中',
    }

    datetime = Field('訂單日期', group=group_order)
    status = ChoiceField('訂單狀態', group=group_order, choices=STATUS_CHOIES)
    attributions = Field('訂單屬性', group=group_order, is_attributions=True)

    group_product = FieldGroup(key='PRODUCT', name='商品')
    product_attributions = Field('商品屬性', group=group_product, is_attributions=True)
    price = Field('商品原價', group=group_product)
    sale_price = Field('商品售價', group=group_product)
    quantity = Field('商品數量', group=group_product)
    total_price = Field('商品小計', group=group_product)
    product_id = Field('商品編號', group=group_product)
    product_name = Field('商品名稱', group=group_product)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orderbase_map = None

    def create_orderbases(self):
        brand_map = {}
        brands = self.team.brand_set.filter(removed=False).values_list('id', 'external_id')
        for brand_id, external_id in brands:
            brand_map[external_id] = brand_id
        orderbase_map = {}
        orders = self.team.purchasebase_set.filter(removed=False).values('id', 'external_id')
        for order in orders:
            orderbase_map[order['external_id']] = PurchaseBase(id=order['id'])
        orderbases_to_create = []
        orderbase = []
        orders_to_update = []
        orderbases_to_update = []
        for order in self.datalist.datalistrow_set.values(
            'order__id', 'order__external_id', 'order__clientbase_id', 'order__datetime', 'order__brand_id',
            'order__status'
        ):

            brand_id = brand_map.get(order['order__brand_id'])
            if order['order__external_id'] in orderbase_map:
                orderbase = orderbase_map[order['order__external_id']]
                update = False
                if brand_id:
                    orderbase.brand_id = brand_id
                    update = True
                if order['order__datetime']:
                    orderbase.datetime = order['order__datetime']
                    update = True
                if order['order__status']:
                    orderbase.status = order['order__status']
                    update = True
                if update and orderbase.id:
                    orderbases_to_update.append(orderbase)
            else:
                orderbase = PurchaseBase(
                    external_id=order['order__external_id'],
                    clientbase_id=order['order__clientbase_id'],
                    datetime=order['order__datetime'],
                    status=order['order__status'],
                    brand_id=brand_id,
                    team=self.team
                )
                orderbase_map[order['order__external_id']] = orderbase
                orderbases_to_create.append(orderbase)
            orders_to_update.append([order['order__id'], orderbase])
        PurchaseBase.objects.bulk_create(orderbases_to_create, batch_size=settings.BATCH_SIZE_M)
        PurchaseBase.objects.bulk_update(orderbases_to_update, ['datetime', 'brand_id'], batch_size=settings.BATCH_SIZE_M)
        orders_to_update = [
            Order(
                id=order_id, purchasebase_id=orderbase.id
            ) for order_id, orderbase in orders_to_update
        ]
        Order.objects.bulk_update(orders_to_update, ['purchasebase_id'], batch_size=settings.BATCH_SIZE_M)
        self.orderbase_map = orderbase_map

    def create_clientbases(self):
        client_map = {}
        clients = list(
            self.team.clientbase_set.filter(removed=False).values_list('external_id', 'id')
        )
        for external_id, client_id in clients:
            client_map[external_id] = ClientBase(id=client_id)
        clientbases_to_create = []
        rows = list(self.datalist.datalistrow_set.values('client__external_id', 'id', 'order__id'))
        for row in rows:
            external_id = row['client__external_id']
            if external_id in client_map:
                clientbase = client_map[external_id]
            else:
                uniq_code = ':'.join([external_id])
                hashcode = hashlib.md5(uniq_code.encode('utf8'))

                uniq_code = int(hashcode.hexdigest(), 16)
                clientbase = ClientBase(uniq_id=uniq_code, external_id=external_id, team=self.team)
                clientbases_to_create.append(clientbase)
                client_map[external_id] = clientbase
            row['clientbase'] = clientbase
        ClientBase.objects.bulk_create(clientbases_to_create, batch_size=settings.BATCH_SIZE_M)
        orders_to_update = []
        for row in rows:
            orders_to_update.append(
                Order(
                    id=row['order__id'],
                    clientbase_id=row['clientbase'].id
                )
            )
        Order.objects.bulk_update(orders_to_update, ['clientbase_id'], batch_size=settings.BATCH_SIZE_M)

    def create_productbases(self):
        product_map = {}
        products = list(
            RetailProduct.objects.filter(team=self.team).filter(removed=False).values_list('external_id', 'id')
        )
        for external_id, product_id in products:
            product_map[external_id] = RetailProduct(id=product_id)
        productbases_to_create = []
        rows = list(self.datalist.datalistrow_set.values('product__external_id', 'id', 'orderrow__id', 'product__name', 'product__price'))
        products_to_update = []
        for row in rows:
            external_id = row['product__external_id']
            if external_id in product_map:
                productbase = product_map[external_id]
                if productbase.id and productbase.id not in products_to_update:
                    products_to_update.append(productbase)
            else:
                productbase = RetailProduct(external_id=external_id, team=self.team, name=row['product__name'], price=row['product__price'])
                product_map[external_id] = productbase
                productbases_to_create.append(productbase)
            row['productbase'] = productbase
        RetailProduct.objects.bulk_create(productbases_to_create, batch_size=settings.BATCH_SIZE_M)
        orders_to_update = []
        for row in rows:
            orders_to_update.append(
                OrderRow(
                    id=row['orderrow__id'],
                    productbase_id=row['productbase'].id
                )
            )
        OrderRow.objects.bulk_update(orders_to_update, ['productbase_id'], batch_size=settings.BATCH_SIZE_M)
        RetailProduct.objects.bulk_update(products_to_update, ['name', 'price'], batch_size=settings.BATCH_SIZE_M)

    def create_orderproducts(self):
        for order in self.orderbase_map.values():
            order.orderproduct_set.all().delete()

        for row in self.datalist.datalistrow_set.values(
            'order__external_id',
            'orderrow__productbase_id',
            'order__clientbase_id',
            'orderrow__refound',
            'orderrow__sale_price',
            'orderrow__quantity'
        ):
            orderbase = self.orderbase_map[row['order__external_id']]
            orderbase.orderproduct_set.create(
                team=self.team,
                productbase_id=row['orderrow__productbase_id'],
                clientbase_id=row['order__clientbase_id'],
                refound=row['orderrow__refound'],
                sale_price = row['orderrow__sale_price'],
                quantity = row['orderrow__quantity'],
                total_price = row['orderrow__sale_price'] * row['orderrow__quantity']
            )

    def calculate_total_price(self):
        orderbases_to_update = []
        for orderbase in self.orderbase_map.values():
            orderproducts = orderbase.orderproduct_set.filter(refound=False)
            total_price = orderproducts.aggregate(total_price=Sum('total_price'))['total_price'] or 0
            orderbase.total_price = total_price
            orderbases_to_update.append(orderbase)
        PurchaseBase.objects.bulk_update(orderbases_to_update, ['total_price'], batch_size=settings.BATCH_SIZE_M)

    def process_raw_records(self):
        self.create_clientbases()
        self.create_orderbases()
        self.create_productbases()
        self.create_orderproducts()
        self.calculate_total_price()
