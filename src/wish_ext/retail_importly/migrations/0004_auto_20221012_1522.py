# Generated by Django 2.2.18 on 2022-10-12 07:22

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('retail_importly', '0003_orderrow_productbase_id'),
    ]

    operations = [
        migrations.RenameField(
            model_name='product',
            old_name='list_price',
            new_name='price',
        ),
    ]
