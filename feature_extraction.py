# coding: utf8
import re

from statflow.mr import Step, Chain
from statflow.common import datetime_from_iso, hash_dict
from statflow.mrjob.service.infinity.prepare_rucenter_clients_new import MainStep as PrepareRuCenterContractsNew
from statflow.mrjob.service.recommender.index_to_region import regions
from collections import defaultdict
from ujson import loads
from dateutil.relativedelta import relativedelta

categories = {
    '1_3_len_domains': -1,
    '4_5_len_domains': -2,
    '5_10_len_domains': -3,
    '10_and_more_len_domains': -4,
    u'VPS/VDS': -5,
    u'SEO': -6,
    u'Конструктор / goMobi': -7,
    u'CMS-хостинг': -8,
    u'Почта': -9,
    u'Secondary': -10,
    u'DNS-мастер': -11,
    u'new gTLD': -12
}

services = {
    u'NIC: Доменное имя RU': 1000,
    u'NIC: Доменное имя РФ': 1093,
    u'NIC: Back-Order RU/РФ Тариф Базовый': 4000,
    u'NIC: COM доменное имя': 1601,
    u'NIC: Пакет RU + RU': 2087,
    u'NIC: DNS-master S': 3211,
    u'NIC: Пакет РФ + RU': 2080,
    u'NIC: Hosting-200': 2264,
    u'NIC: Hosting-100': 2263,
    u'NIC: Hosting-300': 2265,
    u'NIC: Hosting-402': 1560,
    u'NIC: COM.RU': 1273,
    u'NIC: MSK.RU': 1238,
    u'NIC: SPB.RU': 1252,
    u'NIC: Hosting-202': 1933,
    u'NIC: Hosting-102': 1932,
    u'NIC: Hosting-302': 1934,
    u'NIC: Перенаправление домена': 1010,
    u'NIC: Whois-proxy': 1009,
    u'NIC: Доменное имя SU': 1030
}


class FirstStep(Step):

    days_interval = 366

    # TODO move to a new contracts snapshot
    src = [
        PrepareRuCenterContractsNew.dst,
        'snapshot/rucenter-services'
    ]

    dst = 'log/recommender-training-set/{{date}}'

    files = 'log/rucenter-table-services-type-name/{{date}}/-#services.json'

    def get_age(self, birth_date, to_date):
        return to_date.year - birth_date.year - ((to_date.month, to_date.day) < (birth_date.month, birth_date.day))

    def get_zipcode(self, address):
        result = self.pattern_zipcode.search(address)
        if result:
            return result.group(2)

    def get_sex(self, person):
        # works incorrect when name is not russian, or not in russian, not full or some other cases, better to use dictionary of names
        if person is None:
            return
        personal_data = person.split(' ')
        if len(personal_data) == 3 and personal_data[2].endswith(u'вна'):
            return 'female'
        elif len(personal_data) == 3 and personal_data[2].endswith(u'вич'):
            return 'male'

    def get_domain_length_category(self, domain):
        domain = domain.split('.')[0]
        if len(domain) <= 3:
            return '1_3_len_domains'
        elif len(domain) <= 5:
            return '4_5_len_domains'
        elif len(domain) <= 10:
            return '5_10_len_domains'
        else:
            return '10_and_more_len_domains'

    def premap(self):
        self.pattern_zipcode = re.compile('(\D|^)(\d{6})(\D|$)')

    def map(self, key, rec):
        if rec['__type__'] == 'prepared_contracts':
            zipcode = self.get_zipcode(rec['address']) if rec.get('address') is not None else None
            result = {
                'contract_type': rec['contract_type'],
                'internal_legal_type': rec['internal_legal_type'],
                'country': rec['country'],
                'status': rec['status'],
                'date_created': rec['date_created'] if rec.get('date_created') is not None else '2001-01-01',
                'birth_date': rec.get('birth_date'),
                'sex': self.get_sex(rec['contract_pers']),
                'region': str(regions.get(zipcode[:3] if zipcode else '')),
                'is_active': rec['is_active'],
                'subscribed': rec['subscribed']
            }
            yield rec['contract'], ['a', result]
        elif rec['__type__'] == 'rucenter-unified-services-snapshot':
            if not rec.get('serving_now') and not rec.get('is_payed'):
                return
            result = {
                'cost_rur': rec.get('cost_rur'),
                'pay_date': rec.get('pay_date'),
                'serving_now': rec['serving_now'],
                'name': rec['name'],
                'prolong_type': rec['prolong_type'],
                'domain': rec.get('domain'),
                'subgroup': rec['subgroup'],
                'start_date': rec['start_date'],
                'finish_date': rec['finish_date'],
                'is_payed': rec.get('is_payed'),
                'group': rec['group'],
                'zone': rec.get('zone')
            }
            yield rec['contract_name'], ['z', rec.get('pay_date'), result]

    # TODO think how to rebuild it, divide into separate functions
    def get_vector_of_features(self, context_date, records, client_info, data_type='tagged'):
        result = defaultdict(lambda: 0)
        for key in ['1_3_letter_domains', '4_5_letter_domains', '5_10_letter_domains', '10_and_more_letter_domains',
                    'bought_services', 'active_services', 'business_services']:
            result[key] = 0
        count = 0
        last_payment = None
        penultimate_payment = None
        hostings = 0
        domains = 0
        zones = set()
        context_date_iso = context_date.date().isoformat()
        for date, rec in records:
            if date and not 0 < (context_date - date).days <= self.days_interval:
                continue
            penultimate_payment = last_payment if last_payment and date else penultimate_payment
            last_payment = date if date else last_payment
            cost_rur = rec['cost_rur'] if rec.get('cost_rur') else 0
            result['year_payment'] += cost_rur
            if rec['start_date'] < context_date_iso < rec['finish_date'] and rec.get('is_payed'):
                result['active_services'] += 1
            count += 1

            # bought services
            if cost_rur != 0:
                result['bought_services'] += 1

            # set groups and subgroups
            result['gr-' + rec['group']] += 1
            result['sbgr-' + rec['subgroup']] += 1

            # set days from last payment by groups
            if rec['prolong_type'] == 'new' and rec['subgroup'] in categories and cost_rur != 0:
                result['days_from_last_payment_' + rec['subgroup']] = (context_date - date).days
            if rec['prolong_type'] == 'new' and rec['name'] in services and cost_rur != 0:
                result['days_from_last_payment_' + rec['name']] = (context_date - date).days

            # set domain categories and count of zones and days from last payment by groups
            if rec.get('domain') and rec['subgroup'] != u'Дополнительные услуги':
                domain_length_category = self.get_domain_length_category(rec['domain'])
                result[domain_length_category] += 1
                zones.add(rec['zone'].rstrip('.'))
                domains += 1
                if rec['prolong_type'] == 'new' and cost_rur != 0:
                    result['days_from_last_payment_' + domain_length_category] = (context_date - date).days

            #  business services
            if rec['group'] == u'Сервисы для бизнеса':
                result['business_services'] = 1

            # count hostings
            if rec['subgroup'] == u'Хостинг':
                hostings += 1

        if (last_payment is None and result['active_services'] == 0 and data_type == 'tagged') or (data_type == 'untagged' and not client_info['is_active']):
            return
        result['age'] = self.get_age(datetime_from_iso(client_info['birth_date']), context_date) if client_info.get('birth_date') else None
        result['days_from_registration'] = (context_date - datetime_from_iso(client_info['date_created'])).days
        result['average_year_payment'] = result['year_payment'] / count if last_payment else 0
        result['days_from_last_payment'] = (context_date - last_payment).days if last_payment else None
        result['days_from_penultimate_payment'] = (context_date - penultimate_payment).days if penultimate_payment else None
        result['context_date'] = context_date.date().isoformat()
        result['host_dom_prop'] = hostings / domains if domains else 1
        result['has_host'] = hostings != 0
        result['zones'] = len(zones)
        for k in ['internal_legal_type', 'country', 'status', 'sex', 'region', 'subscribed']:
            result[k] = client_info[k]
        return result

    def _get_positive_instances(self, date, context_date, rec):
        if date is None or not 0 < (context_date - date).days <= self.days_interval or rec['prolong_type'] != 'new':
            return set()
        positive_services = set()
        positive_categories = set()
        if rec['subgroup'] in categories:
            positive_categories.add(categories[rec['subgroup']])
        if rec['name'] in services:
            positive_services.add(self.services_type_name[rec['name']])
        if rec.get('domain') and rec['subgroup'] != u'Дополнительные услуги':
            positive_categories.add(categories[self.get_domain_length_category(rec['domain'])])
        return positive_categories | positive_services

    def get_positive_sample(self, context_date, records, client_info):
        for date, rec in records:
            positive_classes = self._get_positive_instances(date, context_date, rec)
            if not positive_classes:
                continue
            result = self.get_vector_of_features(date, records, client_info)
            if result:
                result['target'] = 1
                result['classes'] = positive_classes
                yield result

    def get_negative_sample(self, context_date, records, client_info):
        result = self.get_vector_of_features(context_date - relativedelta(years=1), records, client_info)
        if result is None:
            return
        positive_classes = set()
        for date, rec in records:
            positive_classes |= self._get_positive_instances(date, context_date, rec)
        result['target'] = -1
        negative_calsses = (set(services.values()) | set(categories.values())) - positive_classes
        result['classes'] = negative_calsses
        yield result

    def prereduce(self):
        self.date = datetime_from_iso(self.date)
        services_type_name = {}
        with open('services.json', 'r') as f:
            for line in f:
                service_type_name = loads(line)
                services_type_name[service_type_name['adm_name']] = service_type_name['type']
        self.services_type_name = services_type_name
        self.service_types = set(services_type_name.values())

    def reduce(self, key, records):
        order_flag, client_info = records.next()
        if client_info['contract_type'] == 'PARTNER':
            return
        records = [[datetime_from_iso(date) if date else date, rec] for order_flag, date, rec in records]
        result = self.get_vector_of_features(self.date, records, client_info, 'untagged')
        if result:
            yield key, result
        for result in self.get_negative_sample(self.date, records, client_info):
            yield key, result
        for result in self.get_positive_sample(self.date, records, client_info):
            yield key, result


class FilterClasses(Step):

    dst = 'log/training-sample-by-group/{{date}}'

    def map(self, key, rec):
        classes = rec.pop('classes', None)
        if classes is None:
            return
        for cls in classes:
            rec['classifier_group'] = 'group-' + str(cls)
            yield key, rec

    def reduce(self, key, records):
        result = set()
        for rec in records:
            hash_value = hash_dict(rec)
            if hash_value in result:
                continue
            result.add(hash_value)
            yield rec.pop('classifier_group'), rec


class GetRealServices(Step):

    src = [
        'log/recommender-training-set/{{date}}',
        'classifier/predicted-probabilities/{{date}}'
    ]

    def map(self, key, rec):
        if 'context_date' in rec:
            if 'target' not in rec:
                yield key, rec
        else:
            if rec.get('contract'):
                yield rec['contract'], rec

    def reduce(self, key, records):
        yield key, list(records)


class ExtractFeatures(Chain):

    steps = [FirstStep, FilterClasses, GetRealServices]
