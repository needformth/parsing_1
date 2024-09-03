import requests
import pandas as pd
import numpy as np
import socket
import logging


def authorize() -> str:
    '''Авторизация и получение токена'''
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename="logger.log"
    )
    link = 'http://ip:port/api/v1/application-keys/auth'
    headers = {
        'Content-Type': 'application/json'
    }
    cred = {
        'system': 'login',
        'password': 'password'
    }
    response = requests.post(link, json=cred, headers=headers)
    if response.status_code != 200:
        logging.error('Ошибка подключения к API')
        raise Exception('Ошибка подключения к API')
    token = response.text[1:-1]
    return token

def json_query_people(token: str, n=10000) -> list[dict]:
    '''Получение от сервера файла json-формата'''
    link_2 = 'http://ip:port/api/v1/employees'
    headers_2 = {
        'Authorization': f'Bearer {token}'
    }
    params = {
        'limit': n,
        'excludeFields': 'contactDetails,description,location'
    }
    data = requests.get(link_2, headers=headers_2, params=params)
    if data.status_code != 200:
        logging.error('Ошибка получения списка людей')
        raise Exception('Ошибка получения списка людей')
    return data.json

def DataFrame_people(people_json: list[dict]):
    '''Формирует из json-файла DataFrame, исключая записи без квартир'''
    desired_cols = ['id', 'name', 'departmentId', 'departmentName', 'isBlocked', 'tabId']
    deviants = tuple(filter(lambda x: x[1] != set(desired_cols), \
                         {el['id']: set(el.keys()) for el in people_json}.items()))
    people_json = [dct for dct in people_json if dct['id'] not in [x[0] for x in deviants]]
    df = pd.DataFrame([list(el.values()) for el in people_json], columns=desired_cols)
    df = df.loc[df.departmentName.map(lambda x: 'квартира' in x.lower())]
    return df

def json_query_cards(token: str, n=12000) -> list[dict]:
    '''Получает все карты из запроса на сервер'''
    link_3 = 'http://ip:port/api/v1/cards'
    headers_3 = {
        'Authorization': f'Bearer {token}'
    }
    params_3 = {
        'limit': n,
        'includeFields': 'id,value,holder,mfUid'
    }
    response = requests.get(link_3, headers=headers_3, params=params_3)
    if response.status_code != 200:
        logging.error('Ошибка получения списка карт')
        raise Exception('Ошибка получения списка карт')
    return response.json()

def DataFrame_cards(cards_json: list[dict]):
    '''Формирует из json-файла DataFrame с картами'''
    df_cards = pd.json_normalize(cards_json)
    df_cards.columns = ['id', 'value', 'holderId', 'holderType', 'mfUid']
    df_cards.dropna(subset='holderId', inplace=True)
    df_cards = df_cards.astype({'holderId': int})
    return df_cards

def get_cards_together_with_aparts(people, cards):
    '''Создание DataFrame с квартирами и относящимися к ним картами'''
    res = pd.merge(left=people,
                right=cards,
                how='left',
                left_on='id',
                right_on='holderId',
                suffixes=('_people', '_card'))[['value', 'departmentName']] \
                .dropna() # join'им людей и карты
    res.departmentName = res.departmentName.map(
        lambda flat: int(flat.split('-')[0])) # превращаем квартиру в int
    # форматируем номер карты #
    res.loc[res.value.str.len() > 8, 'value'] = res.loc[res.value.str.len() > 8, 'value'].map(
        lambda s: s[-8:]
        )
    res.loc[res.value.str.len() < 8, 'value'] = res.loc[res.value.str.len() < 8, 'value'].map(
        lambda s: '0' * (8 - len(s)) + s
    )
    return res
def socketing(res) -> None:
    ''' Создание сокета и прослушивание подключений'''
    host = ('', 10000)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(host)
    s.listen()
    print("I'm listening to your connection")
    while True:
        conn, addr = s.accept()
        print('Connected -', addr)
        logging.info(f'Connected - {addr}')
        conn.send(res)

def main():
    # получаем токен #
    token = authorize()

    # получаем json-выгрузку списка людей #
    people_json = json_query_people(token)

    # формируем DataFrame людей из выгрузки #
    df_people = DataFrame_people(people_json) 

    # получаем json-выгрузку списка карт #
    cards_json = json_query_cards(token)
    
    # формируем DataFrame карт из выгрузки #
    df_cards = DataFrame_cards(cards_json) 
    
    # создание DataFrame со связью карта-квартира #
    res = get_cards_together_with_aparts(df_people, df_cards)
    res.to_csv('result.csv') 
    socketing(res)

if __name__ == '__main__':
    main()
