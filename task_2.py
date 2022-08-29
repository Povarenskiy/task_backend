import sqlite3


def repayment():
    """
    Функция находит для платежей долг для оплаты в 2 шага:
    - шаг 1: платеж приоритетно выбирает долг с совпадающим месяцем
    - шаг 2: оставшиеся платежи выбирают самый старый по дате долг
    При этом, платеж может оплатить долг, имеющий более раннюю дату
    """
    result_table = []

    # данны таблиц accrual и payments записываются в соответствующие переменные accrual_list и payments_list
    sql = 'SELECT * FROM accrual ORDER BY month, date'
    cur.execute(sql)
    accrual_list = list(cur.fetchall())

    sql = 'SELECT * FROM payments ORDER BY month, date'
    cur.execute(sql)
    payments_list = list(cur.fetchall())

    # шаг 1 и 2 функции, в зависимости от значения step определяется фильтр для поиска долгов
    for step in ['step 1', 'step 2']:
        i = 0
        while i <= len(payments_list) - 1:
            try:
                payment = payments_list[i]
                if step == 'step 1':
                    match = next(filter(lambda item: (item[1] < payment[1]) and (item[2] == payment[2]), accrual_list))
                else:
                    match = next(filter(lambda item: item[1] < payment[1], accrual_list))
                result_table.append((payment, match))
                accrual_list.remove(match)
                payments_list.remove(payment)
            except StopIteration:
                i += 1

    return result_table, payments_list


# Входныа данные таблицы accrual
accrual = [
    (1, 1, 5),
    (2, 2, 2),
    (3, 3, 3),
    (4, 4, 8),
    (5, 5, 3),
    (6, 6, 12),
    (7, 4, 2)
]

# Входныа данные таблицы payments
payments = [
    (1, 6, 1),
    (2, 7, 2),
    (3, 2, 2),
    (4, 9, 4),
    (5, 10, 5)
]

# Запрос к базе данны sqlite
db = sqlite3.connect('sqlite_task_2.db')
cur = db.cursor()

cur.execute('DROP TABLE IF EXISTS payments')
cur.execute('DROP TABLE IF EXISTS accrual')

cur.execute("CREATE TABLE IF NOT EXISTS accrual(id INT PRIMARY KEY, date DATE, month INT)")
cur.execute("CREATE TABLE IF NOT EXISTS payments(id INT PRIMARY KEY, date DATE, month INT)")

cur.execute("SELECT * FROM accrual")
if cur.fetchone() is None:
    cur.executemany(f"INSERT INTO accrual VALUES(?, ?, ?)", accrual)

cur.execute("SELECT * FROM payments")
if cur.fetchone() is None:
    cur.executemany(f"INSERT INTO payments VALUES(?, ?, ?)", payments)

db.commit()

result = repayment()
print('Таблица соответствий платежей и долгов:', result[0])
print('Список платежей без долга:', result[1])


