from datetime import date

import psycopg2
import requests

from db import db_params


def gather_bali_i_main_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/apptB9TeBEoVa9djt/tblc0nK5MGsmjLrwe',
                                headers={
                                    "Authorization": "Bearer " + 'pat8aakZFTgNDCTlV.76cbff7a26e4da92dcf48b3115eadf8c7004fd1792748aeedcddfc192edfffb5',
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


def prepare_bali_i_main_data(main_data):
    main_data_to_save = []
    for item in main_data:
        data = item['fields']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        try:
            if not data['caption']:
                data['caption'] = None
        except KeyError:
            data['caption'] = None
        try:
            data['Condo ID'] = str(data['Condo ID'])
        except (ValueError, KeyError):
            pass
        try:
            if data['brochure']:
                data['brochure'] = [data['brochure'][0]['url']]
        except KeyError:
            pass
        main_data_to_save.append(data)

    return main_data_to_save


def save_bali_i_main_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
    INSERT INTO general (
        name, address, district, units_number, link_to_condo, brochure, facilities,
        overall_available_units, units, "Condo ID", latest_update, city, companies, caption, link_to_brochure
    )
    VALUES (
        %(name)s, %(address)s, %(district)s, %(units_number)s, %(link_to_condo)s,
        %(brochure)s, %(facilities)s, %(overall_available_units)s,
        %(units)s,
        %(Condo ID)s, %(latest_update)s, %(city)s, %(companies)s, %(caption)s, %(link_to_brochure)s
    ) RETURNING id;
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


def gather_bali_i_units_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/apptB9TeBEoVa9djt/tbl6LHa6RZiGhtGKo',
                                headers={
                                    "Authorization": "Bearer " + 'pat8aakZFTgNDCTlV.76cbff7a26e4da92dcf48b3115eadf8c7004fd1792748aeedcddfc192edfffb5',
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


def prepare_bali_i_units_data(units_data, general_data):
    units_data_to_save = []
    for item in units_data:
        data = item['fields']
        data['unit_id'] = item['id']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        data['Unit_ID'] = data["Unit ID"]
        data['general_id'] = find_dict_with_string(general_data, item['id'])
        try:
            data['num_bedrooms'] = int(data['Bedrooms'][0])
        except (KeyError, ValueError, IndexError):
            data['num_bedrooms'] = None
        try:
            if data['floor_plan_image_links']:
                lst = data['floor_plan_image_links'].split(',')
                lst = [item.strip() for item in lst]
                data['floor_plan_image_links'] = lst
            else:
                data['floor_plan_image_links'] = None
        except KeyError:
            data['floor_plan_image_links'] = None
        units_data_to_save.append(data)

    return units_data_to_save


def find_dict_with_string(lst, search_string):
    for d in lst:
        if d.get('units', None):
            if search_string in d.get('units', []):
                return d['id']


def save_bali_i_units_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO units (
            unit_id, unit_type, latest_update, num_bedrooms, floor_plan_image_links, price_min, size_min,
            available_units, "Unit ID", general_id
        )
        VALUES (
            %(unit_id)s, %(unit_type)s, %(latest_update)s, %(num_bedrooms)s,
            %(floor_plan_image_links)s, %(price_min)s, %(size_min)s, %(available_units)s, %(Unit_ID)s, %(general_id)s
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
