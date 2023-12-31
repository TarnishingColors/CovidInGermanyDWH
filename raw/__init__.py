from datetime import datetime, timedelta


def add_date_column(response):
    response['load_date'] = str((datetime.today() - timedelta(1)).date())
    return response


def data_column_to_list(response):
    data_list = []
    for k, v in response['data'].items():
        v['code'] = k
        data_list.append(v)
    response['data'] = data_list
    return response
