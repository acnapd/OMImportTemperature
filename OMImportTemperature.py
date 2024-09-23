import json
import requests
import argparse
import time
from datetime import datetime, timedelta

parser = argparse.ArgumentParser(description='Получение средней температуры с сайта open-meteo.com')
parser.add_argument("--incity",  required=True, help='название Города, например: "Санкт-Петербург"')
parser.add_argument("--server",  required=True, help='Адрес сервера ЛЭРС УЧЁТ, например: "http://127.0.0.1:10000"')
parser.add_argument("--apiKey",  required=True, help='API ключ учётной записи ЛЭРС УЧЁТ, например: "ASDnklANfjEBF34BJKT-VD"')
parser.add_argument("--destTerritory",  required=True, help='название территории в ЛЭРС УЧЁТ, например: "Санкт-Петербург Город, Город Санкт-Петербург"')
parser.add_argument("--importStart",  required=False, help='(Необязательно). Дата в формате yyyy-MM-dd, начиная с которой будет проводиться импорт. Если не указана, будет импортирована температура за прошлые сутки. Например: "2024-09-05"')
parser.add_argument("--importDays",  required=False, default=1, help='(Необязательно). Если не передан параметр importStart, данные будут импортированы за последний день. Количество дней можно задать с помощью этого параметра. Например, для импорта данных за последние семь дней к параметрам вызова утилиты нужно добавить --importDays "7"')
#parser.add_argument("--missingOnly",  action='store_true' , required=False, help='(Необязательно). Добавьте этот флаг для того чтобы импортировать только ту температуру, которой ещё нет в справочнике. Если параметр не задан, то все существующие в справочнике температуры за заданный интервал будут перезаписаны.')

args = parser.parse_args()


CITY = args.incity
SERVER = args.server
API_KEY_LERS = "Bearer " + str(args.apiKey)
TERRITORY = args.destTerritory
IMPORT_START = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d') if args.importStart==None else args.importStart
IMPORT_DAYS = args.importDays if args.importStart==None else None

START_DAY = args.importStart if args.importStart!=None else (datetime.today() - timedelta(days=int(IMPORT_DAYS))).strftime('%Y-%m-%d')
END_DAY = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

LAT = ""
LON = ""
TZ1 = ""
TZ2 = ""
T_ID = ""


def territory_id_tz():
    global T_ID
    global TZ2
    url = (f"{SERVER}/api/v1/Core/Territories")
    try:
        r = requests.get(url, headers={"Authorization": API_KEY_LERS})
        r.raise_for_status()
        if r.status_code == 200:
            pass
    except:
        print("Ошибка обращения к API ЛЭРС УЧЁТ " + str(r.status_code))
        raise SystemExit
    data = r.json()
    for i in data:
        t_id = i.get('id')
        t_name = i.get('name')
        t_tz = i.get('timeZoneOffset')
        if(t_name==TERRITORY):
            T_ID = t_id
            TZ2 = str(t_tz)


def city_latlon():
    url = (f"https://geocoding-api.open-meteo.com/v1/search?name={CITY}&count=10&language=ru&format=json")
    try:
        r = requests.get(url)
        r.raise_for_status()
        if r.status_code == 200:
            pass
    except:
        print("Ошибка обращения к API open-meteo.com " + str(r.status_code))
        raise SystemExit
    data = r.json()
    global LAT
    global LON
    global TZ1
    LAT = data['results'][0]['latitude']
    LON = data['results'][0]['longitude']
    TZ1 = data['results'][0]['timezone'].replace('/','%2F')


def request():
    url = (f"https://archive-api.open-meteo.com/v1/archive?latitude={LAT}&longitude={LON}&start_date={START_DAY}&end_date={END_DAY}&hourly=temperature_2m&timezone={TZ1}")
    try:
        r = requests.get(url)
        r.raise_for_status()
        if r.status_code == 200:
            pass
    except:
        print("Ошибка обращения к API open-meteo.com " + str(r.status_code))
        raise SystemExit     
    return r.json()


def main():
    city_latlon()
    territory_id_tz()
    data = request()
    temperatures = data["hourly"]["temperature_2m"]
    temperatures = [i for i in temperatures if i is not None]
    days_amount = len(temperatures) // 24
    days = data["hourly"]["time"]
    days = [ x[:-6] for x in days ]
    days = list(dict.fromkeys(days))
    wrong_days = len(days) - days_amount
    days = days[: len(days) - wrong_days]
    days = [s + "T12:00:00." + TZ2 + "Z" for s in days]
    day_temp = [(temperatures[i:i + 24]) for i in range(0, len(temperatures), 24)]
    day_avg_temp = []
    for t in day_temp:
        day_avg_temp.append(round(sum(t) / 24,2))
    wrong_temps = len(day_avg_temp) - days_amount
    day_avg_temp = day_avg_temp[: len(day_avg_temp) - wrong_temps]
    result = list(zip(days, day_avg_temp))
    json_data = []
    cols = ["date","value"]
    l = len(cols)
    j = 0
    
    for i in result:
        d = i
        o = {}
        
        for j in range(l):
            o[cols[j]] = d[j]
        json_data.append(o)
    url = (f"{SERVER}/api/v1/Data/Territories/{T_ID}/Weather")
    print("Импортируются данные с " + datetime.strptime(START_DAY, '%Y-%m-%d').strftime('%d.%m.%Y') + " по " + datetime.strptime(END_DAY, '%Y-%m-%d').strftime('%d.%m.%Y'))
    try:
        r = requests.put(url, data=json.dumps(json_data), headers={"Authorization": API_KEY_LERS, "Content-type": 'application/json'}, timeout=10)
        r.raise_for_status()
        time.sleep(10)
        if r.status_code == 200:
            print("Импорт завершён")
    except:
#        print("Ошибка обращения к API ЛЭРС УЧЁТ " + str(r.status_code))
        raise SystemExit
if __name__ == "__main__":
    main()
