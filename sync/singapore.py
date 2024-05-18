from datetime import date

import psycopg2
import requests
from bs4 import BeautifulSoup

from db import db_params


def gather_main_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/app0pXo7PruFurQjq/tblJObfY0ty6D34wb',
                                headers={"Authorization": "Bearer " + 'patS4Jf9jjQlWTQtY.6a8d3dd5716a685ecc11a084f8a899d266ad34548e1e3571205502fb1176b4e4',
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


def prepare_main_data(main_data):
    main_data_to_save = []
    for item in main_data:
        data = item['fields']
        try:
            if not data['caption']:
                data['caption'] = None
        except KeyError:
            data['caption'] = None
        try:
            data['images'] = [link["url"] for link in data["images"]]
        except KeyError:
            pass
        try:
            data['brochure'] = [link["url"] for link in data["brochure"]]
        except KeyError:
            pass
        try:
            data['site_plans_attachments'] = [link["url"] for link in data["site_plans_attachments"]]
        except KeyError:
            pass
        try:
            data['location_map_attachments'] = [link["url"] for link in data["location_map_attachments"]]
        except KeyError:
            pass
        try:
            del data['images_urls']
        except KeyError:
            pass
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        try:
            if data['floor_plans_urls']:
                lst = data['floor_plans_urls'].split(',')
                lst = [item.strip() for item in lst]
                data['floor_plans_urls'] = lst
            else:
                data['floor_plans_urls'] = None
        except KeyError:
            data['floor_plans_urls'] = None

        try:
            if data['site_plans_urls']:
                lst = data['site_plans_urls'].split(',')
                lst = [item.strip() for item in lst]
                data['site_plans_urls'] = lst
            else:
                data['site_plans_urls'] = None
        except KeyError:
            data['site_plans_urls'] = None

        try:
            desc = gather_condo_desc(data['link_to_condo'])
            if desc:
                data['description'] = desc
        except KeyError:
            pass
        main_data_to_save.append(data)

    return main_data_to_save


def gather_condo_desc(input_str):
    urls = input_str.split(', ')

    for url in urls:
        if 'newlaunches' in url:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                                  'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                                  'scale=3.00; 1170x2532; 463736449) NW/3'}
                r = requests.get(url, headers=headers)
                soup = BeautifulSoup(r.text, 'html.parser')
                desc = ''
                desc_data = soup.find('section', {"id": "project_location"})
                for i, paragraph in enumerate(desc_data.find_all('p')):
                    desc += paragraph.get_text(strip=True) + '\n'
                    if i == 4:
                        break
                return desc
            except Exception:
                return None
    return None


def save_main_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
    INSERT INTO general (
        name, address, district, type, units_number, units_size,
        previewing_start_date, date_of_completion, tenure,
        architect, developer, link_to_condo, brochure, facilities,
        site_plans_attachments, overall_available_units,
        overall_min_unit_size, overall_max_unit_size,
        overall_min_unit_psf, overall_max_unit_psf,
        overall_min_unit_price, overall_max_unit_price,
        location_map_attachments, units, amenities,
        floor_plans_urls, site_plans_urls,
        "Condo ID", latest_update, description, city, companies, selected, caption, link_to_brochure
    )
    VALUES (
        %(name)s, %(address)s, %(district)s, %(type)s, %(units_number)s, %(units_size)s,
        %(previewing_start_date)s, %(date_of_completion)s, %(tenure)s,
        %(architect)s, %(developer)s, %(link_to_condo)s, %(brochure)s, %(facilities)s,
        %(site_plans_attachments)s, %(overall_available_units)s,
        %(overall_min_unit_size)s, %(overall_max_unit_size)s,
        %(overall_min_unit_psf)s, %(overall_max_unit_psf)s,
        %(overall_min_unit_price)s, %(overall_max_unit_price)s,
        %(location_map_attachments)s, %(units)s, %(amenities)s,
        %(floor_plans_urls)s, %(site_plans_urls)s,
        %(Condo ID)s, %(latest_update)s, %(description)s, %(city)s, %(companies)s, %(selected)s, %(caption)s, %(link_to_brochure)s
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


def gather_units_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/app0pXo7PruFurQjq/tblDzvFZ5MoqBLjKl',
                                headers={"Authorization": "Bearer " + 'patS4Jf9jjQlWTQtY.6a8d3dd5716a685ecc11a084f8a899d266ad34548e1e3571205502fb1176b4e4',
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


def prepare_units_data(units_data, general_data):
    units_data_to_save = []
    for item in units_data:
        data = item['fields']
        data['unit_id'] = item['id']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        try:
            if data['floor_plan_image_links']:
                lst = data['floor_plan_image_links'].split(',')
                lst = [item.strip() for item in lst]
                data['floor_plan_image_links'] = lst
            else:
                data['floor_plan_image_links'] = None
        except KeyError:
            data['floor_plan_image_links'] = None

        try:
            data['num_bedrooms'] = int(data['Bedrooms'][0])
        except (KeyError, ValueError, IndexError):
            data['num_bedrooms'] = None
        try:
            del data['General']
        except KeyError:
            pass
        try:
            del data['Book Preview Button']
        except KeyError:
            pass
        try:
            data['Unit_ID'] = data['Unit ID']
        except KeyError:
            data['Unit_ID'] = None

        try:
            data['Condo Images'] = [link["url"] for link in data["Condo Images"]]
        except KeyError:
            pass

        data['general_id'] = find_dict_with_string(general_data, item['id'])

        try:
            data['Address'] = data['Address'][0]
        except (KeyError, IndexError):
            pass

        units_data_to_save.append(data)

    return units_data_to_save


def find_dict_with_string(lst, search_string):
    for d in lst:
        if d.get('units', None):
            if search_string in d.get('units', []):
                return d['id']


def save_units_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO units (
            unit_id, unit_type, all_units, available_units, price_min, size_min, size_max,
            psf_min, condo_images, latest_update, "Unit ID", general_id, num_bedrooms, floor_plan_image_links
        )
        VALUES (
            %(unit_id)s, %(unit_type)s, %(all_units)s, %(available_units)s, %(price_min)s, %(size_min)s,
            %(size_max)s, %(psf_min)s, %(Condo Images)s, %(latest_update)s, %(Unit_ID)s, %(general_id)s, %(num_bedrooms)s, %(floor_plan_image_links)s
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
