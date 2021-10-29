"""
B24 RPA data to DF for BP
"""
import requests
import json
import pandas as pd


FIELD_ITEMS = {
    'UF_RPA_19_NAME': 'Фамилия Имя Отчество',
    'UF_RPA_19_1619555465': 'Гражданство',
    'UF_RPA_19_1619550301': 'Объект',
    'UF_RPA_19_1619540107': 'Номер телефона',
    'UF_RPA_19_1620384867': 'Есть реквизиты карты для перечисления ЗП?',
    'UF_RPA_19_1620384944': 'Есть перевод паспорта?',
    'UF_RPA_19_1620505616': 'Тип процесса',
    'UF_RPA_19_1620382080': 'Дата окончания патента',
    'UF_RPA_19_1620383278': 'Дата окончания регистрации',
    'UF_RPA_19_1623328267': 'Уведомление о приеме отправлено',
    'UF_RPA_19_1623328301': 'Уведомление об увольнении отправлено',
    'UF_RPA_19_1623345779': 'Тип увольнения',
    'UF_RPA_19_1623348668': 'Кто создал увольнение',
    'UF_RPA_19_1624998201': 'Статус вакцинации',
    'UF_RPA_19_1624998435319': 'Дата вакцинации',
    'UF_RPA_19_1619554661': 'Ссылка на сайт проверки Патента(справочно)',
    'UF_RPA_19_1619554711': 'Ссылка на сайт проверки Паспорта(справочно)',
    'UF_RPA_19_1619555123': 'Шаблоны документов(справочно)',
    'UF_RPA_19_1620467355': 'Причина увольнения',
    'UF_RPA_19_1620469319': 'Балл сотрудника',
    'UF_RPA_19_1623066188': 'Исполнитель HR(прием)',
    'UF_RPA_19_1623066254': 'Исполнитель Бух(прием)',
    'UF_RPA_19_1623326770': 'Исполнитель HR(увольнение)',
    'UF_RPA_19_1623326798': 'Исполнитель Бух(увольнение)',
    'UF_RPA_19_1623326819': '(ТЕХ)ДАТА ОФОРМЛЕНИЯ',
    'UF_RPA_19_1623327215': '(ТЕХ)ДАТА УВОЛЬНЕНИЯ',
    'UF_RPA_19_1623348830': 'Статус',
    'UF_RPA_19_1623353024': '(ТЕХ)ID ЭЛЕМЕНТА СПИСКА',
    'UF_RPA_19_1623914270': '(ТЕХ)ДАТА ЗАПУСКА ОФОРМЛЕНИЯ',
    'UF_RPA_19_1623914351': '(ТЕХ)ДАТА ЗАПУСКА УВОЛЬНЕНИЯ',
    'UF_RPA_19_1620483483': '(ТЕХ)ПОВТОРНЫЙ ЦИКЛ'
}

FIELD_TYPES = {
    'id': 'id',
    'title': 'title'
}


def get_url_token():
    url = f"https://oauth.bitrix.info/oauth/token/?grant_type=refresh_token&client_id={B24_CLIENT_ID}&client_secret={B24_CLIENT_SECRET}&refresh_token={B24_REFRESH_TOKEN}"
    response = requests.get(url)
    tmp = json.loads(str(response.text))
    # if 'client_endpoint' not in tmp:
    #     df = pd.DataFrame(tmp)
    #     print(df)
    #     return None, {}
    return tmp['client_endpoint'], {"auth": tmp['access_token']}


def get_rpa_list():
    url, params = get_url_token()
    response = requests.get(url + "rpa.type.list", params=params)
    res_dict = json.loads(response.text)
    # if not response.ok:
    #     df = pd.DataFrame(res_dict)
    #     print(df)
    #     return []
    if 'result' in res_dict and 'types' in res_dict['result']:
        return res_dict['result']['types']
    return []


def get_rpa(id):
    url, params = get_url_token()
    response = requests.get(url + "rpa.type.get?id=" + str(int(id)), params=params)
    return json.loads(response.text)['result']['types']


def get_rpa_items(id):
    url, params = get_url_token()
    response = requests.get(url + "rpa.item.list?typeId=" + str(int(id)), params=params)
    res_dict = json.loads(response.text)
    # if not response.ok:
    #     df = pd.DataFrame(res_dict)
    #     print(df)
    #     return {}
    if 'result' in res_dict and 'items' in res_dict['result'] and res_dict['result']['items']:
        return res_dict['result']['items'][0]
    # print("no", res_dict)
    return {}


def put_list_to_df(res_list):
    df = pd.DataFrame()
    for elem in res_list:
        df = df.append(elem, ignore_index=True)
    return df


rpa_list = get_rpa_list()
df1 = put_list_to_df(rpa_list)
print(df1)

cols = list(FIELD_TYPES.values())
cols.extend(FIELD_ITEMS.values())
df2 = pd.DataFrame(columns=cols)
for rpa in rpa_list:
    rpa_dict = get_rpa_items(rpa['id'])
    # print(rpa_dict)
    res_dict = dict()
    for key in FIELD_TYPES:
        if key in rpa:
            res_dict[FIELD_TYPES[key]] = rpa[key]
    for key in FIELD_ITEMS:
        if key in rpa_dict:
            res_dict[FIELD_ITEMS[key]] = rpa_dict[key]
    df2 = df2.append(res_dict, ignore_index=True)
print(df2)

df1.to_csv("df1.csv", sep=";", encoding="utf-8-sig")