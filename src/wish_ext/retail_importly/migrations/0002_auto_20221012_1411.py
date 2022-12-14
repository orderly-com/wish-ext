# Generated by Django 2.2.18 on 2022-10-12 06:11

import django.contrib.postgres.fields.jsonb
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('team', '0001_initial'),
        ('datahub', '0006_datasource_website'),
        ('importly', '0001_initial'),
        ('retail_importly', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Order',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('c_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('u_at', models.DateTimeField(auto_now=True)),
                ('removed', models.BooleanField(default=False)),
                ('external_id', models.TextField()),
                ('clientbase_id', models.IntegerField(blank=True, null=True)),
                ('status', models.CharField(default=str, max_length=64)),
                ('total_price', models.FloatField(default=0.0)),
                ('datetime', models.DateTimeField(blank=True, null=True)),
                ('attributions', django.contrib.postgres.fields.jsonb.JSONField(default=dict)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='OrderRow',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('c_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('u_at', models.DateTimeField(auto_now=True)),
                ('removed', models.BooleanField(default=False)),
                ('refound', models.BooleanField(default=False)),
                ('attributions', django.contrib.postgres.fields.jsonb.JSONField(default=dict)),
                ('sale_price', models.FloatField(default=0.0)),
                ('quantity', models.IntegerField(default=1)),
                ('total_price', models.FloatField(default=0.0)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.RemoveIndex(
            model_name='product',
            name='retail_impo_datasou_f3b91d_idx',
        ),
        migrations.RemoveIndex(
            model_name='product',
            name='retail_impo_title_ce73ef_idx',
        ),
        migrations.RemoveIndex(
            model_name='product',
            name='retail_impo_team_id_4297db_idx',
        ),
        migrations.RemoveIndex(
            model_name='product',
            name='retail_impo_team_id_e5f238_idx',
        ),
        migrations.RemoveField(
            model_name='product',
            name='author',
        ),
        migrations.RemoveField(
            model_name='product',
            name='content',
        ),
        migrations.RemoveField(
            model_name='product',
            name='datetime',
        ),
        migrations.RemoveField(
            model_name='product',
            name='path',
        ),
        migrations.RemoveField(
            model_name='product',
            name='status',
        ),
        migrations.RemoveField(
            model_name='product',
            name='title',
        ),
        migrations.AddField(
            model_name='product',
            name='list_price',
            field=models.FloatField(default=0.0),
        ),
        migrations.AddField(
            model_name='product',
            name='name',
            field=models.CharField(default=str, max_length=64),
        ),
        migrations.AddField(
            model_name='orderrow',
            name='datalist',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='importly.DataList'),
        ),
        migrations.AddField(
            model_name='orderrow',
            name='datalistrow',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='importly.DataListRow'),
        ),
        migrations.AddField(
            model_name='orderrow',
            name='datasource',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datahub.DataSource'),
        ),
        migrations.AddField(
            model_name='orderrow',
            name='team',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='team.Team'),
        ),
        migrations.AddField(
            model_name='order',
            name='datalist',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='importly.DataList'),
        ),
        migrations.AddField(
            model_name='order',
            name='datalistrow',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='importly.DataListRow'),
        ),
        migrations.AddField(
            model_name='order',
            name='datasource',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datahub.DataSource'),
        ),
        migrations.AddField(
            model_name='order',
            name='team',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='team.Team'),
        ),
        migrations.AddIndex(
            model_name='orderrow',
            index=models.Index(fields=['datalistrow'], name='retail_impo_datalis_9111da_idx'),
        ),
        migrations.AddIndex(
            model_name='order',
            index=models.Index(fields=['datalistrow'], name='retail_impo_datalis_dec3ab_idx'),
        ),
    ]
