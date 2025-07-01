# необходимые библиотеки
import pandas as pd
import numpy as np

import datetime as dt
from datetime import datetime


import random

import mimesis
from mimesis import Person
from mimesis.enums import Gender
from mimesis import Datetime
from mimesis.locales import Locale
from mimesis.builtins import RussiaSpecProvider
from mimesis import Datetime

#функция формирования тестовой таблицы
def create_table_test():
    datetime_mim = Datetime()
    lst_name = []
    lst_date = []
    person = Person(Locale.RU)
    ru = RussiaSpecProvider()

    # генерация списка должностей, родительского индекса
    position = ['CEO'] + ['Manager'] * 10 + ['Team Lead'] * 100 + ['Senior Developer'] * 1000 + ['Developer'] * 49000
    lst_parent_id = [0] + [1] * 10 + list(range(2, 12)) * 10 + list(range(13, 113)) * 10 + list(range(114, 1114)) * 49
    cnt_people_company = len(position)

    # генерация мужских имен (с предположением что мужчин 60%  от общего количества  сотрудников компании)
    # генерация даты приянтия на работу
    for i in range(0, int(cnt_people_company * 60 / 100)):
        family = person.surname(gender=Gender.MALE)
        name = person.name(gender=Gender.MALE)
        last_name = ru.patronymic(gender=Gender.MALE)
        full_name = f'{family} {name} {last_name}'
        lst_name.append(full_name)
        date = datetime_mim.date()
        lst_date.append(date)

    # генерация женских имен (с предположением что мужчин 40%  от общего количества  сотрудников компании)
    # генерация даты приянтия на работу
    ru = RussiaSpecProvider()
    for i in range(0, int(cnt_people_company * 40 / 100) + 1):
        family = person.surname(gender=Gender.FEMALE)
        name = person.name(gender=Gender.FEMALE)
        last_name = ru.patronymic(gender=Gender.FEMALE)
        full_name = f'{family} {name} {last_name}'
        lst_name.append(full_name)
        lst_date.append(date)

    random.shuffle(lst_name)

    # генерация заработной платы в зависимости от должности
    lst_salary = []
    for i in position:
        if i == 'CEO':
            lst_salary.append(random.randint(550000, 1000000))
        elif i == 'Manager':
            lst_salary.append(random.randint(450000, 600000))
        elif i == 'Team Lead':
            lst_salary.append(random.randint(350000, 500000))
        elif i == 'Senior Developer':
            lst_salary.append(random.randint(150000, 400000))
        else:
            lst_salary.append(random.randint(100000, 200000))

    # генерация id
    # lst_id = [i for i in range(1, 50001)]

    # датасет state_company
    df_state_company = pd.DataFrame()
    df_state_company['full_name'] = lst_name
    df_state_company['date_employment'] = lst_date
    df_state_company['salary'] = lst_salary
    df_state_company['position'] = position
    df_state_company['parent_id'] = lst_parent_id
    df_state_company = df_state_company.sort_values('parent_id', ascending=True)
    # df_state_company['id'] = lst_id

    df_state_company['parent_id'] = df_state_company['parent_id'].replace(0, None)
    df_state_company = df_state_company[['full_name', 'position', 'date_employment', 'salary', 'parent_id']]
    df_state_company.to_csv('state_company.csv', index=False, encoding='utf-8')
    print('Фиктивная таблица данных о сотрудниках компании сформирована')

#пользовательские функции
# функция формирования sql запросовpy
def formation_request(expression, command='select', full_name_update=''):
    def sql_simple(expression,
                   sign,
                   command,
                   list_of_positions=
                   ['CEO', 'MANAGER', 'TEAMLEAD', 'SENIORDEVELOPER', 'DEVELOPER']):
        if expression.isalpha() == True:
            if expression in list_of_positions:
                if command != 'update':
                    return (f" REPLACE(UPPER(position),' ', '') {sign} '{expression}' ")
                else:
                    return f"Обновить должность невозможно"
            else:
                if command != 'update':
                    return (f" REPLACE(UPPER(full_name),' ', '') {sign} '{expression}' ")
                else:
                    return (f"full_name = '{expression}' ")

        elif expression.isdigit() == True:
            if ((len(expression) == 8)
                    and (int(expression[0]) in [1, 2])
                    and (int(expression[4:6]) <= 12)
                    and (int(expression[6:]) <= 31)):
                if command != 'update':
                    return (f" date_employment {sign} '{expression}' ")
                else:
                    return (f"date_employment = '{expression}' ")
            else:
                if command != 'update':
                    return (f" salary {sign} {expression} ")
                else:
                    return (f"salary = {expression} ")
        else:
            return ('')

    expression = expression.replace(' ', '').upper()
    sign = []
    union = []
    list_expression = []
    sql_total = ""

    # определение основной команды
    if command == 'select':
        sql_first_part = (f'SELECT full_name AS "ФИО", '
                          f'position AS "Должность", '
                          f'salary AS "Заработная плата", '
                          f'date_employment AS "Дата принятия на работу" '
                          f'FROM state_company '
                          f'WHERE')
        sql_end = ';'
    elif command == 'delete':
        sql_first_part = (f'DELETE '
                          f'FROM state_company '
                          f'WHERE ')
        sql_end = ';'
    elif command == 'update':
        sql_first_part = (f'UPDATE state_company '
                          f'SET ')
        sql_end = (
            f"WHERE REPLACE(UPPER(full_name),' ', '') = '{full_name_update.upper().replace(' ', '')}';")

    # определенние операндов  и союзов
    for i in expression:
        if command == 'update':
            union.append(',')
        else:
            if i == '|':
                union.append('OR')
                expression = expression.replace(i, '&')
            elif i == '&':
                union.append('AND')

    list_expression = expression.split('&')
    for i in range(len(list_expression)):
        for j in list_expression[i]:
            if j in ['<', '>', '=', '<=', '>=']:
                list_expression[i] = list_expression[i].replace(j, '')
                sign.append(j)
            else:
                sign.append("=")

        # формирование запроса sql
        if len(list_expression) == 1:
            sql = sql_first_part + sql_simple(list_expression[0], sign[0], command=command) + sql_end
        else:
            for i in range(len(list_expression) - 1):
                sql_second_part = sql_simple(list_expression[i], sign[i], command=command) + union[i]
            sql_total = sql_total + sql_second_part
        sql = sql_first_part + sql_total + sql_simple(list_expression[-1], sign[-1], command=command) + sql_end
        return sql


def formation_sql_reqursive(name, goal_sign):
    # запрос формирование таблицы подчиненных
    if goal_sign == '-':
        sql = (
            f"WITH RECURSIVE cte_subordinates AS ("
            f"SELECT 1 as Level, position, full_name, id, parent_id, CAST(id AS TEXT) AS path "
            f"FROM state_company "
            f"WHERE REPLACE(UPPER(full_name),' ','') = '{name}' "
            f"UNION ALL "
            f"SELECT level +1, sc.position, sc.full_name, sc.id, sc.parent_id, cs.Path || '->' || sc.id "
            f"FROM state_company sc "
            f"JOIN cte_subordinates cs ON sc.parent_id = cs.id) "
            f"SELECT  level, position, full_name, path "
            f"FROM cte_subordinates "
            f"ORDER BY path"
        )
    elif goal_sign == '0':
        sql = (
            f"WITH RECURSIVE cte_subordinates AS ("
            f"SELECT 1 as Level, position, full_name, id, parent_id, CAST(id AS TEXT) AS path "
            f"FROM state_company "
            f"WHERE  parent_id is null "
            f"UNION ALL "
            f"SELECT level +1, sc.position, sc.full_name, sc.id, sc.parent_id, cs.Path || '->' || sc.id "
            f"FROM state_company sc "
            f"JOIN cte_subordinates cs ON sc.parent_id = cs.id) "
            f"SELECT  level, position, full_name, path "
            f"FROM cte_subordinates "
            f"WHERE level+1 "
        )
    # запрос на формирование таблицы начальства
    else:
        sql = (
            f"WITH RECURSIVE cte_subordinates AS ("
            f"SELECT 1 as Level, position, full_name, id, parent_id, CAST(position AS TEXT) AS path_position, "
            f"CAST(full_name AS TEXT) AS path_name "
            f"FROM state_company "
            f"WHERE  parent_id is null "
            f"UNION ALL "
            f"SELECT level +1, sc.position, sc.full_name, sc.id, sc.parent_id, cs.path_position || ' -> ' || sc.position, "
            f"cs.path_name || ' -> ' || sc.full_name "
            f"FROM state_company sc "
            f"JOIN cte_subordinates cs ON sc.parent_id = cs.id) "
            f"SELECT id, path_position, path_name "
            f"FROM cte_subordinates "
            f"WHERE REPLACE(UPPER(full_name),' ','') = '{name}' "
        )
    return sql

# функция печати лерева субординации
def print_subordinates(sql_rows, goal_sign, level_subardinates=5):
    if goal_sign == '-':
        max_len = len(','.join(max(sql_rows)[1:3])) + level_subardinates * 4
        print(f" {'=' * max_len}")
        print(f" {sql_rows[0][1]} ({sql_rows[0][2]})")
        print(f" {'=' * max_len}")
        for i in range(1, len(sql_rows) - 1):
            if len(sql_rows[i][3]) <= len(sql_rows[i + 1][3]):
                print(f"{' | ' * (sql_rows[i][0] - 2)} |-- {sql_rows[i][1]} ({sql_rows[i][2]})")
            else:
                print(f"{' | ' * (sql_rows[i][0] - 2)} └── {sql_rows[i][1]} ({sql_rows[i][2]})")
                print(f"{' | ' * (sql_rows[i][0] - 2)}")
        print(
            f"{' | ' * (sql_rows[len(sql_rows) - 1][0] - 2)} └── {sql_rows[len(sql_rows) - 1][1]} ({sql_rows[len(sql_rows) - 1][2]})")
        print(f" {'=' * max_len}")
    else:
        lst_positions = sql_rows[0][1].split(' -> ')
        lst_name = sql_rows[0][2].split(' -> ')
        print(f"{'Должность':^50} {'ФИО':^50}")
        for i in range(len(lst_positions) - 1):
            print(f"{'=' * 100}")
            print(f"{lst_positions[i]:^50}|{lst_name[i]:^50}")
            print(f"{'=' * 100}")
            print(f" ")
            print(f"{'↓':^50}|{'↓':^50}")
            print(f" ")
        print(f"{'=' * 100}")
        print(f"{lst_positions[len(lst_positions) - 1]:^50}|{lst_name[len(lst_positions) - 1]:^50}")
        print(f"{'=' * 100}")
