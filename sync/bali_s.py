from datetime import date, datetime

import psycopg2
import requests

from db import db_params


def find_dict_with_string(lst, search_string):
    for d in lst:
        current_date = datetime.today().date()
        if d.get('units', None):
            if d.get('latest_update', None) == current_date:
                if search_string in d.get('units', []):
                    return d['id']


def gather_sbali_main_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/appNdDlpH4kjKiOop/tblwCRcgS6oKTU2Ba',
                                headers={
                                    "Authorization": "Bearer " + 'patZD84AZAURJt1Ya.4dfd128258cb34a54c1f0789789941f6904a9add26013d7b9bc5e53d9ecb995b',
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


def prepare_sbali_main_data(main_data):
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
            if data['commission, %']:
                data['commission'] = data['commission, %']
        except (ValueError, KeyError):
            pass
        try:
            if data['developer links']:
                data['developer_links'] = data['developer links']
        except (ValueError, KeyError):
            pass
        try:
            if data['developer website']:
                data['developer_website'] = data['developer website']
        except (ValueError, KeyError):
            pass
        try:
            if data['area, ha']:
                data['area'] = data['area, ha']
        except (ValueError, KeyError):
            pass
        try:
            if data['brochure']:
                data['brochure'] = [data['brochure'][0]['url']]
        except KeyError:
            pass
        main_data_to_save.append(data)

    return main_data_to_save


def save_sbali_main_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
    INSERT INTO general (
        name, address, district, units_number, link_to_condo, brochure, facilities,
        overall_available_units, units, "Condo ID", latest_update, city, companies, developer, location, commission, 
        developer_links, developer_website, date_of_completion, tenure, area, overall_min_unit_size, overall_min_unit_price, caption
    )
    VALUES (
        %(name)s, %(address)s, %(district)s, %(units_number)s, %(link_to_condo)s,
        %(brochure)s, %(facilities)s, %(overall_available_units)s,
        %(units)s, %(Condo ID)s, %(latest_update)s, %(city)s, %(companies)s, 
        %(developer)s, %(location)s, %(commission)s, %(developer_links)s, %(developer_website)s, %(date_of_completion)s,
         %(tenure)s, %(area)s, %(overall_min_unit_size)s, %(overall_min_unit_price)s, %(caption)s
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
        for record in formatted_data:
            try:
                cursor.execute(insert_sql, record)
            except Exception:
                continue

        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def gather_sbali_units_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/appNdDlpH4kjKiOop/tblqnbChXpe4RChPk',
                                headers={
                                    "Authorization": "Bearer " + 'patZD84AZAURJt1Ya.4dfd128258cb34a54c1f0789789941f6904a9add26013d7b9bc5e53d9ecb995b',
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


def prepare_sbali_units_data(units_data, general_data):
    units_data_to_save = []
    for item in units_data:
        data = item['fields']
        data['unit_id'] = item['id']
        try:
            data['date_of_completion'] = data['date_of_completion'][0]
        except (KeyError, IndexError):
            pass
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        data['Unit_ID'] = data["Unit ID"]
        data['general_id'] = find_dict_with_string(general_data, item['id'])
        try:
            data['roi'] = data['ROI, %']
        except (KeyError, ValueError):
            data['roi'] = None
        try:
            data['net_rent_per_month'] = data['net rent per month']
        except (KeyError, ValueError):
            data['net_rent_per_month'] = None
        try:
            data['num_bedrooms'] = int(data['Bedrooms'][0])
        except (KeyError, ValueError, IndexError):
            data['num_bedrooms'] = None
        try:
            data['num_bathrooms'] = int(data['Bathrooms'][0])
        except (KeyError, ValueError, IndexError):
            data['num_bathrooms'] = None
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


def save_sbali_units_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
            INSERT INTO units (
                unit_id, unit_type, latest_update, num_bedrooms, floor_plan_image_links, price_min, size_min,
                available_units, "Unit ID", general_id, developer, price_max, roi, all_units,
                net_rent_per_month, income_per_year, rental_yield, beach_min, beach_m, size_min_building, size_min_land, 
                private_pool, car_parking, Comments, date_of_completion, address, facilities
            )
            VALUES (
                %(unit_id)s, %(unit_type)s, %(latest_update)s, %(num_bedrooms)s,
                %(floor_plan_image_links)s, %(price_min)s, %(size_min)s, %(available_units)s, %(Unit_ID)s, %(general_id)s,
                %(developer)s, %(price_max)s, %(roi)s, %(all_units)s, %(net_rent_per_month)s,
                %(income_per_year)s, %(rental_yield)s, %(beach_min)s, %(beach_m)s, %(size_min_building)s, %(size_min_land)s,
                %(private_pool)s, %(car_parking)s, %(Comments)s, %(date_of_completion)s, %(address)s, %(facilities)s
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
        for record in formatted_data:
            try:
                cursor.execute(insert_sql, record)
            except Exception:
                continue

        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()
