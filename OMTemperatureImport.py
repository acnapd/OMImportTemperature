import json
import requests
import argparse
from datetime import datetime, timedelta


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Получение средней температуры с сайта open-meteo.com')
    parser.add_argument("--incity", required=True,
                        help='название Города, например: "Санкт-Петербург"')
    parser.add_argument("--server", required=True,
                        help='Адрес сервера ЛЭРС УЧЁТ, например: "http://127.0.0.1:10000"')
    parser.add_argument("--apiKey", required=True,
                        help='API ключ учётной записи ЛЭРС УЧЁТ, например: "ASDnklANfjEBF34BJKT-VD"')
    parser.add_argument("--destTerritory", required=True,
                        help='название территории в ЛЭРС УЧЁТ, например: "Санкт-Петербург Город, Город Санкт-Петербург"')
    parser.add_argument("--importStart", required=False,
                        help='(Необязательно). Дата в формате yyyy-MM-dd, начиная с которой будет проводиться импорт. Если не указана, будет импортирована температура за прошлые сутки. Например: "2024-09-05"')
    parser.add_argument("--importDays", required=False, default=1, help='(Необязательно). Если не передан параметр importStart, данные будут импортированы за последний день. Количество дней можно задать с помощью этого параметра. Например, для импорта данных за последние семь дней к параметрам вызова утилиты нужно добавить --importDays "7"')
    parser.add_argument("--missingOnly", action='store_true', required=False,
                        help='(Необязательно). Добавьте этот флаг для того чтобы импортировать только ту температуру, которой ещё нет в справочнике. Если параметр не задан, то все существующие в справочнике температуры за заданный интервал будут перезаписаны.')
    return parser.parse_args()


def get_territory_id(server, api_key, territory):
    url = f"{server}/api/v1/Core/Territories"
    r = requests.get(url, headers={"Authorization": api_key})
    for entry in r.json():
        if entry.get('name') == territory:
            return entry.get('id')
    return None


def get_city_latlon(city):
    url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}&count=10&language=ru&format=json"
    r = requests.get(url)
    results = r.json().get('results', [])
    if results:
        return results[0]['latitude'], results[0]['longitude'], results[0]['timezone'].replace('/', '%2F')
    return None, None, None


def fetch_weather_data(lat, lon, start_day, end_day, tz):
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={start_day}&end_date={end_day}&hourly=temperature_2m&timezone={tz}"
    return requests.get(url).json()


def calculate_average_temperatures(temperatures, start_day, end_day):
    valid_temps = [t for t in temperatures if t is not None]
    days_amount = len(valid_temps) // 24
    days = [start_day + timedelta(days=i) for i in range(days_amount)]
    day_avg_temp = [round(sum(valid_temps[i:i + 24]) / 24, 2)
                    for i in range(0, len(valid_temps), 24)]
    return list(zip(days, day_avg_temp))


def import_temperatures(server, api_key, t_id, temperature_data):
    url = f"{server}/api/v1/Data/Territories/{t_id}/Weather"
    try:
        r = requests.put(url, data=json.dumps(temperature_data), headers={
                         "Authorization": api_key, "Content-type": 'application/json'}, timeout=10)
        r.raise_for_status()
        if r.status_code == 200:
            print("Импорт завершён")
        else:
            print(f"Ошибка {r.status_code}")
    except requests.exceptions.RequestException:
        print("Нет ответа от API ЛЭРС УЧЁТ")


def main():
    args = parse_arguments()
    CITY, SERVER, API_KEY_LERS, TERRITORY = args.incity, args.server, f"Bearer {args.apiKey}", args.destTerritory
    IMPORT_START = args.importStart or (
        datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    IMPORT_DAYS = args.importDays if args.importStart is None else None
    START_DAY = datetime.strptime(IMPORT_START, '%Y-%m-%d')
    END_DAY = datetime.today() - timedelta(days=1)

    LAT, LON, TZ1 = get_city_latlon(CITY)
    if not LAT or not LON or not TZ1:
        print("Не удалось получить координаты города.")
        return

    T_ID = get_territory_id(SERVER, API_KEY_LERS, TERRITORY)
    if not T_ID:
        print("Не удалось получить ID территории.")
        return

    weather_data = fetch_weather_data(LAT, LON, START_DAY.strftime(
        '%Y-%m-%d'), END_DAY.strftime('%Y-%m-%d'), TZ1)
    temperatures = weather_data.get("hourly", {}).get("temperature_2m", [])
    if not temperatures:
        print("Не удалось получить данные о температуре.")
        return

    temperature_data = calculate_average_temperatures(
        temperatures, START_DAY, END_DAY)
    if args.missingOnly:
        # не реализовано
        pass

    json_data = [{'date': day.strftime(
        '%Y-%m-%dT00:00:00Z'), 'value': temp} for day, temp in temperature_data]
    print(
        f"Импортируются данные с {START_DAY.strftime('%d.%m.%Y')} по {END_DAY.strftime('%d.%m.%Y')}")
    import_temperatures(SERVER, API_KEY_LERS, T_ID, json_data)


if __name__ == "__main__":
    main()
