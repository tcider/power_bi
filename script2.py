"""
список задач по сотрудникам Инженерной службы для оценки ключевых показателей работы
"""

import requests
import json
import pandas as pd
import datetime
from urllib.parse import urlencode


# Получать ли отдельным запросом теги? (0 или 1)
TAG_FLAG = 0
# Сколько последних хаписей получать, если None - То ВСЕ, или указываем число
LIMIT = None


TASKS_FIELDS = {
    'id': "ID",
    'title': 'Название',
    'description': 'Описание',
    'group': 'Проект',
    'priority': 'Приоритет',
    'status': 'Статус',
    'creator': 'Постановщик',
    'responsible': 'Исполнитель',
    'accomplices': 'Соисполнитель',
    'auditors': 'Наблюдатель',
    'createdDate': "Дата создания",
    'activityDate': "Дата конечного завершения",
    'dateStart': 'Дата начала',
    'deadline': 'Крайний срок',
    'startDatePlan': 'Планируемая дата начала',
    'endDatePlan': 'Планируемая дата завершения',
    'closedDate': 'Дата завершения',
    'timeEstimate': 'Затраченое время',
    'durationFact': 'Длительность',
    'mark': 'Оценка',
    'ufCrmTask': 'Привязка к элементам CRM',
    'tag': 'Хештег',
    'ufAuto778320703796': 'Ремонт в мастерской'
}
USERS_FIELDS = ['ID', 'TITLE', 'DESCRIPTION', 'GROUP_ID', 'PRIORITY', 'STATUS', 'CREATED_BY', 'RESPONSIBLE_ID', 'ACCOMPLICES', 'AUDITORS', 'CREATED_DATE', 'ACTIVITY_DATE', 'DATE_START', 'DEADLINE',
                'START_DATE_PLAN', 'END_DATE_PLAN', 'CLOSED_DATE', 'TIME_ESTIMATE', 'DURATION_FACT', 'MARK', 'UF_CRM_TASK', 'UF_AUTO_778320703796']

USER_DF_FIELDS = {
    'ID': 'ID',
    'EMAIL': 'EMAIL',
    'NAME': 'NAME',
    'LAST_NAME': 'LAST_NAME',
    'SECOND_NAME': 'SECOND_NAME'
}

ENUM_FIELDS = ['priority', 'status', 'mark']
DATE_FIELDS = ['createdDate', 'activityDate', 'dateStart', 'deadline', 'startDatePlan', 'endDatePlan', 'closedDate']
NAMES_FIELDS = ['group', 'creator', 'responsible']
DICT_NAMES_FILEDS = ['accomplices', 'auditors']


def init_token(): # Для инициализации токена
    """
    CODE_URL get in browser
    """
    url2 = f"https://oauth.bitrix.info/oauth/token/?grant_type=authorization_code&client_id={B24_CLIENT_ID}&client_secret={B24_CLIENT_SECRET}&code={CODE}"
    response = requests.get(url2)
    print(response.text)
    res_dict = json.loads(str(response.text))
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(res_dict, f)


def get_url_token():
    url = f"https://oauth.bitrix.info/oauth/token/?grant_type=refresh_token&client_id={B24_CLIENT_ID}&client_secret={B24_CLIENT_SECRET}&refresh_token={B24_REFRESH_TOKEN}"
    response = requests.get(url)
    tmp = json.loads(str(response.text))
    # print(tmp)
    # if 'client_endpoint' not in tmp:
    #     df = pd.DataFrame(tmp)
    #     print(df)
    #     return None, {}
    return tmp['client_endpoint'], {"auth": tmp['access_token']}


def convert_date(date_str):
    if not date_str:
        return None
    dt_obj = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S+02:00')
    new_dt_obj = dt_obj + datetime.timedelta(hours=1)
    return new_dt_obj.strftime('%Y-%m-%dT%H:%M:%S+03:00')


def get_tasks(): # Для отладки
    url, params = get_url_token()
    start = 0
    res = []
    # fields = ",".join(list(TASKS_FIELDS))
    # fields = 'ufCrmTask'
    # params['select'] = [['id'],'[DATE_START]', 'CREATED_DATE']

    while True:
        response = requests.get(url + f"tasks.task.list?filter[TAG]=Заявка в инженерную службу&start={start}&select[id, title]=True", params=params)
        # response = requests.get(url + f"tasks.task.list?filter[TAG]=Заявка в инженерную службу&select['ufCrmTask']&start={start}",  params=params)
        # print(response.text)
        res_dict = json.loads(response.text)
        res_list = res_dict['result']['tasks']
        res.extend(res_list)
        if 'next' in res_dict:
            start = res_dict['next']
        else:
            break
    print(res)
    return res


def tasks_fields(): # Для отладки
    url, params = get_url_token()
    response = requests.get(url + f"tasks.task.getFields", params=params)
    res_dict = json.loads(response.text)['result']['fields']
    return res_dict


def get_task(task_id): # Для отладки
    # UF_CRM_TASK
    url, params = get_url_token()
    params["select[]"] = USERS_FIELDS
    # select = urlencode(str(USERS_FIELDS))
    response = requests.get(url + f"tasks.task.get?taskId={task_id}", params=params)
    res_dict = json.loads(response.text)
    print(res_dict)
    return res_dict


def get_task_tag(task_id):
    """
    Для получения тэгов конкретной задачи необходимо передать параметр /rest/task.item.gettags.xml?TASK_ID=3&auth=18tci5kga6v12g8okzm5r26sv0n9is84. Запрос может быть как ID, так и TASK_ID. Принципиально, чтобы этот параметр был первым. В ответ вернётся {"result":["TAG1","TAG2","ETC..."]}.
    """
    url, params = get_url_token()
    response = requests.get(url + f"task.item.gettags?taskId={task_id}", params=params)
    res_dict = json.loads(response.text)
    if 'result' in res_dict:
        return res_dict['result']
    return []


def get_enum_dict(field_name, value, all):
    field_name = field_name.upper()
    if field_name in all:
        field_dict = all[field_name]
        if field_dict['type'] == 'enum' and value in field_dict['values']:
            value = field_dict['values'][value]
    return value


def get_users_df(): # Для вывода таблицы пользователей - не сипользую
    url, params = get_url_token()
    start = 0
    df2 = pd.DataFrame(columns=USER_DF_FIELDS.values())
    while True:
        response = requests.get(url + f"user.get?start={start}", params=params)
        res_dict = json.loads(response.text)
        for elem in res_dict['result']:
            res_dict2 = dict()
            for key in USER_DF_FIELDS:
                if key in elem:
                    res_dict2[USER_DF_FIELDS[key]] = elem[key]
            df2 = df2.append(res_dict2, ignore_index=True)
        if not 'next' in res_dict:
            break
        start = res_dict['next']
    return df2


def get_users_dict():
    url, params = get_url_token()
    start = 0
    res = {}
    while True:
        response = requests.get(url + f"user.get?start={start}", params=params)
        res_dict = json.loads(response.text)
        for elem in res_dict['result']:
            res[elem['ID']] = elem["LAST_NAME"] + ' ' + elem['NAME']
        if not 'next' in res_dict:
            break
        start = res_dict['next']
    return res


df = pd.DataFrame(columns=TASKS_FIELDS.values())
url, params = get_url_token()
start = 0
i = 0
tasks_fields_dict = tasks_fields()
users_dict = get_users_dict()
params["select[]"] = USERS_FIELDS

while True:
    response = requests.get(url + f"tasks.task.list?filter[TAG]=Заявка в инженерную службу&start={start}", params=params)
    res_dict = json.loads(response.text)
    res_list = res_dict['result']['tasks']

    for elem in res_list:
        # print(elem)
        res_dict2 = dict()
        for key in TASKS_FIELDS:
            if key in elem or key == 'tag':
                if key in ENUM_FIELDS:
                    value = get_enum_dict(key, elem[key], tasks_fields_dict)
                elif key in DATE_FIELDS:
                    value = convert_date(elem[key])
                elif key in NAMES_FIELDS:
                    if 'name' in elem[key]:
                        value = elem[key]['name']
                    else:
                        value = ''
                elif key in DICT_NAMES_FILEDS:
                    value = ''
                    for elem2 in elem[key]:
                        value += users_dict[elem2] + ', '
                elif key == "tag":
                    if TAG_FLAG:
                        value = get_task_tag(elem['id'])
                    else:
                        value = 'Инженерная служба'
                else:
                    value = elem[key]
                res_dict2[TASKS_FIELDS[key]] = value
                i += 1

        df = df.append(res_dict2, ignore_index=True)

    if 'next' in res_dict:
        start = res_dict['next']
    else:
        break

    if LIMIT and i >= LIMIT:
        break

# print(get_users_df())
# df.to_csv("df.csv", sep=";", encoding="utf-8-sig")
print(df)