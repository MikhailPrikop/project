#загрузка необходимых библиотек и функций
import psycopg2
from tabulate import tabulate
import os.path
import configparser
config = configparser.ConfigParser()
import datetime as dt
import sys

from  functions import formation_request
from functions import formation_sql_reqursive
from functions import print_subordinates
from functions import create_table_test

#Основной код
#Эмблема приветствие
iline = [
    '#####  #####           #####  #####               #####  ###############',
    '#   #  #   #           #   #  #   #               #   #  #             #',
    '#   #  #   #           #   #  #   #  #            #   #  #   ###########',
    '#   #  #   #           #   #  #   #    #          #   #  #   #          ',
    '#   #  #   #           #   #  #   #  #    #       #   #  #   ###########',
    '#   #  #   #           #   #  #   #    #    #     #   #  #             #',
    '#   #  #   #           #   #  #   #       #   #   #   #  #   ###########',
    '#   #  #   #           #   #  #   #         #   # #   #  #   #          ',
    '#   #  #   ########### #   #  #   #           #   #   #  #   ###########',
    '#   #  #             # #   #  #   #             # #   #  #             #',
    '#####  ############### #####  #####               #####  ###############',
]
for i in iline:
    print(i)

print(" ")
# работа с файлом конфигурации
path = os.path.abspath('ILINE.exe')
path = path[:-9]
path = (path + 'ILINE.ini')
if os.path.isfile(path) == False:
    print('ОБРАТИТЕ ВНИМАНИЕ')
    print('**********************************************************************')
    print('В корневой папке ILINE отсутствует файл конфигурации ILINE.ini')
    print('Подключение к базе данных будет осуществлено по следующим параметрам ')
    print("dbname = postgres\n"
          "user = postgres\n"
          "host = localhost\n"
          "port = 5432")
    print('***********************************************************************')
    print(' ')
    # параметры подключения к базе данных postgres
    dbname = 'postgres'
    user = 'postgres'
    host = 'localhost'
    port = '5432'

    #формирование файла конфигурации по умолчанию
    config.read(path)
    config.add_section('database')
    config.set('database', 'dbmane', 'postgres')
    config.set('database', 'user', 'postgres')
    config.set('database', 'host', 'localhost')
    config.set('database', 'port', '5432')
    with open('ILINE.ini', 'w') as f:
        config.write(f)

else:
    config.read(path)
    DB = config.has_section('database')
    dbname = config.has_option('database', 'dbmane')
    user = config.has_option('database', 'user')
    host = config.has_option('database', 'host')
    port = config.has_option('database', 'port')
    sections = [DB]
    options = [dbname, user, host, port]

    if all(sections) != True or all(options) != True:
            print('ОБРАТИТЕ ВНИМАНИЕ')
            print('****************************************************************************************')
            print('Подключение к базе данных будет осуществлено по следующим параметрам ')
            print("dbname = postgres\n"
                  "user = postgres\n"
                  "host = localhost\n"
                  "port = 5432")
            print('******************************************************************************************')
            # параметры подключения к базе данных postgres
            dbname = 'postgres'
            user = 'postgres'
            host = 'localhost'
            port = '5432'
    else:
        dbname = config.get('database', 'dbmane')
        user = config.get('database', 'user')
        host = config.get('database', 'host')
        port = config.get('database', 'port')
        # параметры подключения к базе данных postgres
        dbname = dbname
        user = user
        host = host
        port = port

try:
    conn = psycopg2.connect(
            dbname= dbname,
            user= user,
            host= host,
            port=  port)
    print('Соединение с базой данных установлено')
    print(' ')
    connect_db = True
except:
    print('ОШИБКА!!!')
    print('****************************************************************************************')
    print('Невозможно установить соединение с базой данных')
    print("Для выхода из программы введите 'ВЫХОД' или 'EXIT'")
    print('****************************************************************************************')
    connect_db = False
    while True:
        user_input = input()
        if user_input.upper() == 'ВЫХОД' or user_input.upper() == 'EXIT':
            break

if connect_db == True:
    cur = conn.cursor()
    conn.autocommit = True

    print(" ")
    print('Добро пожаловать в консольное приложение ILINE!!!')
    print(f"{'*' * 120}")
    print('- для поиска данных введите ФИО интересующего Вас сотрудника')
    print('- для обновления информации в базе данных нажмите клавишу "R"')
    print("- для добавления информации в базу данных  нажмите клавишу '+'")
    print("- для отображения иерархии сотрудников введите I")
    print("- для удаления информации из базы данных нажмите клавишу '-'")
    print("- для выхода из программы введите 'ВЫХОД' или 'EXIT'")
    print("- для формирования фиктивной тестовой базы данных о сотрудниках введите 'TEST'")
    print("- для вызова памятки введите ?")
    print(f"{'*' * 120}")
    print(" ")

    while True:
        print(" ")
        user_input = input().upper()
        # добавление данных
        if user_input == 'ДОБАВИТЬ' or user_input == '+':
            list_position = ['CEO', 'MANAGER', 'TEAMLEAD', 'SENIORDEVELOPER', 'DEVELOPER']
            # ФИО сотрудника
            while True:
                full_name = input('Введите ФИО сотрудника: ').upper()
                if full_name == '' or full_name.isalpha() == True:
                    print('ФИО сотрудника не может быть пустым либо числовым значением.')
                else:
                    break
            # Должность сотрудника
            while True:
                print(" ")
                job_title = input('Введите должность сотрудника: ').upper()
                if job_title.replace(' ', '') not in list_position:
                    print('Указанной Вами должности в компании не существует.')
                else:
                    break

            # дата принятия на работу
            while True:
                try:
                    print(" ")
                    date_input = str(input("В формате YYYYMMDD введите дату принятия сотрудника на работу: "))
                    dt.datetime.strptime(date_input, '%Y%m%d').date()
                    break
                except ValueError:
                    print("Неправильный ввод даты принятия сотрудника на работу. Попробуйте ещё раз.")

            # заработная плата сотрудника
            while True:
                try:
                    print(" ")
                    salary = int(input("Введите установленную заработную плату сотрудника: "))
                    break
                except ValueError:
                    print("Заработная плата должна быть числовым значением")

                    # выберите непосредственного начальника сотрудника
            potential_boss = list_position.index(job_title.replace(' ', '')) - 1
            if potential_boss == -1:
                parent_id = 'NULL'
            else:
                sql = (f'SELECT position AS "Должность", '
                       f'full_name AS "ФИО",'
                       f'id AS "Шифр подразделения" '
                       f'FROM state_company '
                       f"WHERE REPLACE(UPPER(position), ' ', '') = '{list_position[potential_boss]}'")

                cur.execute(sql)
                sql_rows = cur.fetchall()
                print(" ")
                print('Выберите непосредственного начальника сотрудника')
                print(" ")
                print(tabulate(sql_rows,
                               headers=['Должность', 'ФИО сотрудника', 'Шифр подразделения'],
                               tablefmt='grid'))
                print(" ")
                stop = True
                while True:
                    print(" ")
                    parent_id = input('Введите соответствующий шифр поразделения ')

                    for row in sql_rows:
                        for j in row:
                            if parent_id == str(j):
                                stop = False

                    if stop == False:
                        break

            sql = (f"INSERT INTO state_company(full_name, position, date_employment, salary, parent_id) "
                    f"VALUES ('{full_name}', '{job_title}',  '{date_input}',  {salary},  {parent_id})")
            try:
                cur.execute(sql)
                print(" ")
                print('ИНФОРМАЦИЯ  ДОБАВЛЕНА')
                print(" ")
            except:
                print('ОШИБКА ДОБАВЛЕНИЯ ДАННЫХ')

        # удаление данных
        elif user_input == 'УДАЛИТЬ' or user_input == '-':
            print(" ")
            input_value = input('Введите значение необходимое для удаления ')
            sql = formation_request(input_value)
            try:
                cur.execute(sql)
                sql_rows = cur.fetchall()
                if sql_rows != []:
                    print(tabulate(sql_rows,
                                   headers=['ФИО сотрудника', 'Должность', 'Заработная плата', 'Дата принятия на работу'],
                                   tablefmt='grid')
                          )
                    delete = True
                else:
                    print('УКАЗАННЫЕ ЗНАЧЕНИЯ НЕ НАЙДЕНЫ')
                    delete = False

                if delete == True:
                    print(" ")
                    print("ОБРАТИТЕ ВНИМАНИЕ ЧТО ПРИ УДАЛЕНИИ ВЫБРАННОГОСОТРУДНИКА ")
                    print("БУДЕТ УДАЛЕНА ИНФОРМАЦИЯ О ВСЕХ СОТРУДНИКАХ НАХОДЯЩИХСЯ НЕГО В ПОДЧИНЕНИИ")
                    print(" ")
                    print('Вы действительно хотите удалить выбранную информацию.')
                    print(f"{'=' * 120}")
                    print('- для подтверждения удаления нажмите клавишу "Enter')
                    print('- для выхода из меню удаления нажмите любую другую клавишу ')
                    print(f"{'=' * 120}")
                    print(" ")
                    user_answer = input()
                    if user_answer == '':
                        sql = formation_request(input_value, command='delete')
                        cur.execute(sql)
                        sql = formation_request(input_value)
                        cur.execute(sql)
                        sql_rows = cur.fetchall()
                        if sql_rows == []:
                            print("ДАННЫЕ УСПЕШНО УДАЛЕНЫ")
                        else:
                            print('ОШИБКА УДАЛЕНИЯ ДАННЫХ')
            except:
                print("ОШИБКА!!! ВОЗМОЖНО ВВЕДЕНЫ НЕДОПУСТИМЫЕ СИМВОЛЫ")

        # обновление данных
        elif user_input == 'ОБНОВИТЬ' or user_input == 'R' or user_input == 'К':
            print(" ")
            print(f"{'=' * 120}")
            print("Для обновления информации  о сотруднике компании введите его ФИО")
            print(f"{'=' * 120}")
            input_value = input()
            if input_value !="":
                sql = formation_request(input_value)
                cur.execute(sql)
                sql_rows = cur.fetchall()
                if sql_rows != []:
                    print(tabulate(sql_rows,
                                   headers=['ФИО сотрудника', 'Должность', 'Заработная плата', 'Дата принятия на работу'],
                                   tablefmt='grid')
                          )
                    update = True
                else:
                    print(f"ИНФОРМАЦИЯ О {input_value} ОТСУТСТВУЕТ")
                    update = False

                if update == True:
                    while True:
                        print(' ')
                        print('Введите конечные значения')
                        print('Для выхода нажмите клавишу Enter')
                        print(' ')
                        update_input = input()

                        if update_input != "":
                            sql_update = formation_request(update_input, command='update', full_name_update=input_value)
                            try:
                                cur.execute(sql_update)
                                print('ОБНОВЛЕНИЕ ДАННЫХ ПРОШЛО УСПЕШНО')

                            except:
                                print("ОШИБКА!!!")
                        else:
                            break
        # отображения иерархии
        elif user_input == 'I':
            name_user = input('Введите ФИО интересующего Вас сотрудника ').replace(' ', '').upper()
            while True:
                print(" ")
                print("-" * 100)
                print("- для вывода непосредственного начальства сотрудника введите '+'")
                print("- для вывода подчиненных сотрудника введите '-'")
                print("- для выхода из меню нажмите клавишу Enter")
                print("-" * 100)
                goal_sign = input()
                if goal_sign != '':
                    sql = formation_sql_reqursive(name_user, goal_sign)
                    try:
                        cur.execute(sql)
                        sql_rows = cur.fetchall()
                        if sql_rows != []:
                            print_subordinates(sql_rows, goal_sign)

                        else:
                            print("ИНФОРМАЦИЯ НЕ НАЙДЕНА")
                            break
                    except:
                        print("ОШИБКА!!! ВОЗМОЖНО ВВЕДЕНЫ НЕДОПУСТИМЫЕ СИМВОЛЫ")
                        break
                else:
                    break

        # вызов памятки
        elif user_input == '?':
            print(" ")
            print("ПАМЯТКА".center(120, '*'))
            print(f"{'-' * 120}")
            print('- для поиска данных введите ФИО интересующего Вас сотрудника')
            print('- для обновления информации в базе данных нажмите клавишу "R"')
            print("- для добавления информации в базу данных  нажмите клавишу '+'")
            print("- для удаления информации из базы данных нажмите клавишу '-'")
            print("- для формирования фиктивной тестовой базы данных о сотрудниках введите 'TEST'")
            print("- для вызова памятки введите ?")
            print(f"{'-' * 120}")
            print(" ")
            print('ПОИСК'.center(120, '='))
            print("По умолчанию поиск и фильтрация данных осуществляются по введеным значениям")
            print("Для поиска допустимо использовать:")
            print(f"{'-' * 120}")
            print("- операторы сравнения '>,<,>=,<=,='")
            print("- логические операторы '&(И), |(ИЛИ)'")
            print(f"{'-' * 120}")
            print("ПРИМЕР:")
            print(f"{'-' * 120}")
            print("ввод '> 20230101 & < 20240101' - позволит отразить")
            print("информацию о сотрудниках принятых на работу с 20230101 по 20240101")
            print(f"{'-' * 120}")
            print(f"{'=' * 120}")
            print(" ")
            print("ОБНОВЛЕНИЕ".center(120, "="))
            print('Для обновления необходимо ввести конечные значения')
            print('Если значений несколько используйте между ними символ &')
            print('ПРИМЕР')
            print(f"{'-' * 120}")
            print('ввод "manger & 2024324" - обновит имеющиеся должность и заработную плату сотрудника')
            print(f"{'-' * 120}")
            print(f"{'=' * 120}")
            print(" ")
            print(f"УДАЛЕНИЕ".center(120, '='))
            print("Удаление информации из базы данных по умолчанию осуществяется по ФИО сотрудника.")
            print("Для удаления информации по другим критериям укажите соответсвующее значение")
            print("ПРИМЕР:")
            print(f"{'-' * 120}")
            print('ввод "Manager" - позволит удалить информацию о сотрудниках на должности Manager')
            print(f"{'-' * 120}")
            print(f"{'=' * 120}")
            print(f"{'*' * 120}")
            print(" ")
        # пустое значение
        elif user_input == '':
            print('')
        # Выход
        elif user_input.upper() == 'EXIT' or user_input.upper() == 'ВЫХОД':
            break
        # поиск и фильтрация данных
        elif user_input.upper() == 'TEST':
            create_table_test()
            path_csv = os.path.abspath('state_company.csv')
            sql = (f"CREATE TABLE state_company ( "
            f"id INT PRIMARY KEY NOT NULL GENERATED ALWAYS AS IDENTITY,"
            f"full_name VARCHAR(200), "
            f"position VARCHAR(200) NOT NULL, "
            f"date_employment DATE, "
            f"salary INT, "
            f"parent_id INT, "
            f"CONSTRAINT state_company_parent_id_fk FOREIGN KEY(parent_id) "
            f"REFERENCES state_company(id) ON DELETE CASCADE "
            f");"
    
            f"COPY state_company(full_name, position, date_employment, salary, parent_id) "
            f"FROM '{path_csv}' DELIMITER ',' CSV HEADER; "
            f"SELECT * FROM state_company " )
            print(path_csv)
            try:
                cur.execute(sql)
                sql_rows = cur.fetchall()
            except:
                print("СФОРМИРОВАТЬ ТЕСТОВУЮ ТАБЛИЦУ НЕ УДАЛОСь")


        else:
                sql = formation_request(user_input)
                try:
                    cur.execute(sql)
                    sql_rows = cur.fetchall()
                    if sql_rows != []:
                        print(tabulate(sql_rows,
                                       headers=['ФИО сотрудника', 'Должность', 'Заработная плата', 'Дата принятия на работу'],
                                       tablefmt='grid'))
                    else:
                        print("ИНФОРМАЦИЯ НЕ НАЙДЕНА")
                except:
                    print("ОШИБКА!!! ВОЗМОЖНО ВВЕДЕНЫ НЕДОПУСТИМЫЕ СИМВОЛЫ")

        cur.close()
        conn.close()
        sys.exit()
