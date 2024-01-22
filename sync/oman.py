from datetime import date

import psycopg2
import requests

from db import db_params


def gather_oman_main_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/appnWWKxXiSjmDLCR/tbl6laBo8kWKvfZPC',
                                headers={
                                    "Authorization": "Bearer " + 'patozqZihGw0H9iIY.c571db7a2ffbf522ccd1f7970679514f2f003422cdcfa54c9a947803bc93ef49',
                                    "Content-Type": "application/json",
                                    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                                                  'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                                                  'scale=3.00; 1170x2532; 463736449) NW/3'}, params=params)
        if response.status_code == 200:
            data = response.json()
            records = data['records']
            all_records.extend(records)

            if 'offset' in data:
                params['offset'] = data['offset']
            else:
                break
        else:
            print(f"Failed to retrieve records (status code: {response.status_code}): {response.text}")
            break
    return all_records


def prepare_oman_main_data(main_data):
    main_data_to_save = []
    for item in main_data:
        data = item['fields']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        try:
            data['Condo ID'] = str(data['Condo ID'])
        except (ValueError, KeyError):
            pass

        main_data_to_save.append(data)

    return main_data_to_save


def save_oman_main_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
    INSERT INTO general (
        name, link_to_condo, district, overall_available_units,
        overall_min_unit_price, overall_min_unit_psf, "Condo ID", latest_update, city, companies
    )
    VALUES (
        %(name)s, %(link_to_condo)s,
        %(district)s, %(overall_available_units)s,
        %(overall_min_unit_price)s, %(overall_min_unit_psf)s,
        %(Condo ID)s, %(latest_update)s, %(city)s, %(companies)s
    );
    """

    formatted_data = []
    for record in data:
        formatted_record = {}
        for key in insert_sql.split('%(')[1:]:
            key = key.split(')s')[0]
            if key not in record:
                record[key] = None
            formatted_record[key] = record[key]
        formatted_data.append(formatted_record)

    try:
        cursor = connection.cursor()
        cursor.executemany(insert_sql, formatted_data)
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()
