# coding: utf8
import logging

from airflow.operators import BaseOperator
from statflow.autowiring import WiredOperator
from datetime import date, datetime
from statflow.plugins.decorators import context_hadoop
from statflow.config import config
from statflow.oracle import OracleClient
from sqlalchemy import create_engine
from statflow.common import HiveMappingMixin
from time import time

logger = logging.getLogger(__name__)


class ProxyDBObject():

    def __init__(self, connection_config, db_name=None):
        url = connection_config.get('url')
        if url.startswith('oracle'):
            self.engine = OracleClient(
                connection_config.get('host'),
                connection_config.get('port'),
                connection_config.get('user'),
                connection_config.get('password'),
                connection_config.get('service_name')
            )
            self.type = 'oracle'
            self.db = db_name
        elif url.startswith('mysql'):
            self.engine = create_engine(url).connect()
            self.type = 'mysql'
        else:
            raise RuntimeError("Unsupported db type")

    def get_full_table_name(self, table_name):
        if self.type == 'oracle' and self.db is not None:
            return self.db + '.' + table_name
        else:
            return table_name

    def execute_query(self, query):
        logger.info("SQL: %s", query)
        if self.type == 'oracle':
            result = self.engine.iterquery(query)
            for row in result:
                yield row
        elif self.type == 'mysql':
            result = self.engine.execute(query)
            for row in result:
                yield dict(row)

    def close(self):
        self.engine.close()


class ImportTableOperator(BaseOperator, WiredOperator):

    def __init__(self, table, id_column, columns, **kwargs):
        self.table = table
        self.columns = columns
        self.id_column = id_column
        super(ImportTableOperator, self).__init__(**kwargs)

    brand = None
    connection_config_path = None
    db_name = None
    tables = []

    def wired_dst(self, context):
        dst = ['log/%s-table-%s/%s' % (self.brand, self.table.replace('_', '-'), context['execution_date'].date().isoformat())]
        if not self.id_column:
            dst.append(dst[-1] + '/-')
        return dst

    @classmethod
    def wired_instances(cls, dag):
        operators = []
        for table, id_column, columns in cls.tables:
            operators.append(cls(table, id_column, columns, task_id='Import' + cls.brand.title() + table.title().replace('_', ''), dag=dag, pool='import_' + cls.brand))
        return operators

    def _get_data(self, context, table_name, columns, id_column=None, from_id=None, to_id=None):
        columns = ','.join(columns)
        condition = ''
        if id_column is not None:
            condition = """WHERE {} >= {} AND {} < {}""".format(id_column, from_id, id_column, to_id)
        query = """SELECT {} FROM {} t {}""".format(columns, table_name, condition)
        logger.info("Start fetching data from table %s", table_name)
        result = context['db'].execute_query(query)
        rec = {
            '__type__': 'rucenter-%s' % table_name,
            '__ts__': time(),
        }
        for row in result:
            for k, v in row.iteritems():
                if isinstance(v, datetime) or isinstance(v, date):
                    row[k] = v.isoformat()
            row.update(rec)
            yield row

    def _get_min_max_id(self, context, table_name, id_column):
        logger.info("Get min id and max id from %s", table_name)
        minmax_query = """SELECT min({}) "min", max({}) "max" FROM {}""".format(id_column, id_column, table_name)
        result = context['db'].execute_query(minmax_query)
        for row in result:
            return row['min'], row['max']
        raise RuntimeError("There is no id")

    def import_table(self, context, table_name, dst, columns, id_column=None, from_id=None, to_id=None):
        hadoop = context['hadoop']
        if hadoop.exists(dst):
            hadoop.rm(dst, recursive=True)
        hadoop.mkdir(dst)
        if id_column:
            while from_id <= to_id:
                offset_id = from_id + 400000
                data = self._get_data(context, table_name, columns, id_column, from_id, offset_id)
                logger.info("Put data from table %s to hdfs from %s to %s", table_name, from_id, offset_id)
                hadoop.put_data(data, dst + '/' + str(from_id))
                from_id = offset_id
        else:
            data = self._get_data(context, table_name, columns)
            logger.info("Put data from table %s to hdfs", table_name)
            hadoop.put_data(data, dst)

    @context_hadoop
    def execute(self, context):
        execution_date = context['execution_date'].date()
        conn_conf = config.get_config(self.connection_config_path)
        context['db'] = ProxyDBObject(conn_conf, self.db_name)
        table_name = context['db'].get_full_table_name(self.table)

        from_id = None
        to_id = None
        if self.id_column is not None:
            from_id, to_id = self._get_min_max_id(context, table_name, self.id_column)
        dst = 'log/%s-table-%s/%s' % (self.brand, self.table.replace('_', '-'), execution_date.isoformat())
        self.import_table(context, table_name, dst, self.columns, self.id_column, from_id, to_id)
        context['db'].close()


class ImportRcData(HiveMappingMixin, ImportTableOperator):

    brand = 'rucenter'
    connection_config_path = 'statflow.databases.ru_center'
    db_name = 'OPS$AUTODBM'
    tables = [
        ('services_type_name', None, ['*']),
        ('coll_rec', None, ['*']),
        ('services', 'id', ['*']),
        ('acc_rec', 'acc_rec_id', ['*']),
        ('bills_fact', 'bill_ind', ['*']),
        ('dogovor', 'ind', ['t.*', 'pack_contract.getNumber(t.ind) as sp_contract_name', 'pack_contract.getNameR(t.ind) as sp_full_name',
                            'pack_contract.getNumber(t.owner) as sp_owner', 'getaccountsum(t.ind) as sp_balance']),
        ('invoiceitems', 'invoice_id', ['*']),
        ('order_items', 'oi_id', ['order_id', 'oi_status', 'service_type', 'oi_id', 'oi_status', 'oi_type', 'blocked', 'domain_name']),
        ('orders', 'order_id', ['contract_id', 'order_id', 'submitted', 'status']),
        ('object_item_service_contract_a', 'oi_id', ['oi_id', 'service_type', 'contract_id', 'id', 'domain']),
        ('bonus_plat_acc', None, ['acc_rec_id']),
        ('blocks', None, ['acc_rec_id']),
        ('currency_rate', None, ['*']),
        ('fm_services', None, ['*']),
        ('fm_descr', None, ['*']),
        ('services_type_group_v', None, ['*']),
        ('order_item_data', 'oi_id', ['data', 'field', 'oi_id']),
        ('rrp_contacts_v', None, ['nic_hdl', 'email', 'contract_id']),
        ('rrp_domains_v', None, ['registrant', 'domain']),
    ]

    @classmethod
    def hive_mapping(cls, context):
        execution_date = context['execution_date'].date().isoformat()
        table_config = [
            {
                'location': 'log/rucenter-table-dogovor/%s' % execution_date,
                'table': 'rucenter_table_dogovor',
                'columns': ['ind INT', 'num FLOAT', 'type INT', 'owner INT', 'endday STRING', 'flags STRING']
            },
            {
                'location': 'log/rucenter-table-order-items/%s' % execution_date,
                'table': 'rucenter_table_order_items',
                'columns': ['order_id INT', 'oi_status INT', 'service_type INT']
            },
            {
                'location': 'log/rucenter-table-orders/%s' % execution_date,
                'table': 'rucenter_table_orders',
                'columns': ['contract_id INT', 'order_id INT']
            },
            {
                'location': 'log/rucenter-table-fm-descr/%s' % execution_date,
                'table': 'rucenter_table_fm_descr',
                'columns': ['fm_id INT', 'login STRING']
            },
            {
                'location': 'log/rucenter-table-fm-services/%s' % execution_date,
                'table': 'rucenter_table_fm_services',
                'columns': ['service_type INT', 'fm_id INT']
            },
            {
                'location': 'log/rucenter-table-services-type-group-v/%s' % execution_date,
                'table': 'rucenter_table_services_type_group_v',
                'columns': ['type INT', 'grp INT', 'grp2 INT']
            },
        ]
        return table_config


class ImportHcData(ImportTableOperator):

    brand = 'hostcomm'
    connection_config_path = 'statflow.databases.hostcomm'
    db_name = 'billing'
    tables = [
        ('contragent', None, ['*']),
        ('contract', None, ['*']),
        ('personal_data', None, ['*']),
        ('sale', 'sale_id', ['sale_id', 'contract_id', 'amount', 'lock_id', 'operation_date', 'from_date', 'to_date']),
        ('lock1', 'lock_operation_id', ['lock_operation_id', 'operation_type', 'defrayal_id', 'amount', 'operation_date', 'from_date', 'to_date']),
        ('defrayal', 'defrayal_id', ['defrayal_id', 'service_instance_id', 'amount', 'order_id', 'status', 'service_name', 'task_action', 'rate_id']),
        ('service_instance', 'service_instance_id', ['*']),
        ('service', None, ['*']),
        ('rate', 'rate_id', ['*']),
        ('doc_sale_items', 'sale_id', ['*']),
        ('documents', 'document_id', ["document_id", "document_type"]),
    ]


class ImportHcSubscriptions(ImportTableOperator):
    brand = 'hostcommSubscriptions'
    connection_config_path = 'statflow.databases.hostcomm_subscriptions'
    db_name = 'notification_manager'
    tables = [
        ('client_account_category_permissions', 'client_account_id', ['*']),
        ('client_account_permissions', 'client_account_id', ['*']),
    ]
