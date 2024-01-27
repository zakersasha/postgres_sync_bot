import psycopg2

from db import db_params


def delete_old_general_data(city):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    try:
        if type(city) == str:
            delete_sql = """
                        SELECT id FROM general 
                        WHERE city = %s;
                    """

            cursor.execute(delete_sql, (city,))
            ids_to_delete = cursor.fetchall()
            flat_deleted_ids = [item for sublist in ids_to_delete for item in sublist]

            delete_sql = """
                        DELETE FROM units
                        WHERE general_id = ANY(%s);
                    """

            cursor.execute(delete_sql, (flat_deleted_ids,))
            connection.commit()

            delete_sql = """
                                    DELETE FROM general 
                                    WHERE city = %s;
                                """

            cursor.execute(delete_sql, (city,))
            connection.commit()
        else:
            query = """
                    SELECT id FROM general
                    WHERE city = ANY(%s);
                    """

            cursor.execute(query, (city,))
            ids_to_delete = cursor.fetchall()

            flat_deleted_ids = [item for sublist in ids_to_delete for item in sublist]

            delete_sql = """
                        DELETE FROM units
                        WHERE general_id = ANY(%s);
                    """

            cursor.execute(delete_sql, (flat_deleted_ids,))
            connection.commit()

            delete_sql = """
                                                DELETE FROM general 
                                                WHERE city = ANY(%s);
                                            """

            cursor.execute(delete_sql, (city,))
            connection.commit()

        connection.commit()
        print("Rows deleted successfully.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def delete_units_by_general_ids(deleted_ids):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    try:
        flat_deleted_ids = [item for sublist in deleted_ids for item in sublist]

        delete_sql = """
            DELETE FROM units
            WHERE general_id = ANY(%s);
        """

        cursor.execute(delete_sql, (flat_deleted_ids,))
        connection.commit()

        print("Units deleted successfully.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
