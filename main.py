import os
import cx_Oracle
import pandas as pd
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()


def get_files_content():
    """
    This method fetch the query and file path and stores in a dictionary.
    """
    file_content_dict = {}
    try:
        sql_folder_location = os.getenv('SQL_FILES_FOLDER_LOCATION')
        files_in_folder = os.listdir(sql_folder_location)
        print(f'Files present in folder {sql_folder_location} are: {files_in_folder}')
        for file in files_in_folder:
            if file.endswith('sql'):
                with open(f'{sql_folder_location}\\{file}') as f:
                    file_content_dict[f'{sql_folder_location}\\{file}'] = f.read().replace('\n', ' ').replace(';', '')
            else:
                print(f"'{file}' is not a SQL file, hence not fetching query from it.")
    except Exception as e:
        print(f"Exception occurred while fetching queries from the file: {e}")
    return file_content_dict


def create_db_connection():
    """
    This method returns the connection between db and python script
    """
    cx_Oracle.init_oracle_client(lib_dir=os.getenv('INSTANT_CLIENT_FOLDER_LOCATION'))
    dsn_tns = cx_Oracle.makedsn(os.getenv('DB_HOSTNAME'), os.getenv('DB_PORT'), service_name=os.getenv('SERVICE_NAME'))
    connection = cx_Oracle.connect(os.getenv('DB_USERNAME'), os.getenv('DB_PASSWORD'), dsn_tns, cx_Oracle.SYSDBA)
    print("Connection established successfully")
    return connection


def hit_sql_queries_and_store_output_in_excel(connection, file_content_dict):
    """
    This method hit the query one by one and stores the result in the same folder in form of xlsx file.
    :param connection
    :param file_content_dict
    """
    for file_path, query in file_content_dict.items():
        print(f"Hitting query for {file_path}")
        dataframe = pd.read_sql(query, con=connection)
        current_date_time = str(f'({(date.today().strftime("%d %b"))} {(datetime.now().strftime("%H_%M_%S"))})')
        excel_location = f'{file_path[:-4]}_output_{current_date_time}.xlsx'
        dataframe.to_excel(excel_location)
        print(f"Output saved in location: {excel_location}")
    connection.close()
    print("DB Connection closed")


def main():
    """
    This is the main method. Execution will start from here.
    """
    file_content_dict = get_files_content()
    connection = create_db_connection()
    hit_sql_queries_and_store_output_in_excel(connection, file_content_dict)


if __name__ == '__main__':
    main()
