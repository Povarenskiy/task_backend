import pymongo
import datetime


# Задается формат строки для преобразование str в datetime
time_format = '%Y-%m-%dT%H:%M:%S'

# Входные данные: коллекция пользователей 'Account'
context = [{
    'number': '7800000000000',
    'name': 'Пользователь №1',
    'sessions': [
        {
            'created_at': datetime.datetime.strptime('2016-01-01T00:00:00', time_format),
            'session_id': '6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgxZc',
            'actions': [
                {
                    'type': 'read',
                    'created_at': datetime.datetime.strptime('2016-01-01T01:20:01', time_format),
                },
                {
                    'type': 'read',
                    'created_at': datetime.datetime.strptime('2016-01-01T01:21:13', time_format),
                },
                {
                    'type': 'create',
                    'created_at': datetime.datetime.strptime('2016-01-01T01:33:59', time_format),
                }
            ] ,
        }
    ]
}]

# подключение к базе данных Mongodb
client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['mongodb_task_1']

collection = db['Account']
result = db['Result']

# Записываем входные данные в бд
collection.insert_many(context)

# Создаем агрегационный запрос, результат записывается в бд Result
collection.aggregate(
    [
        {'$unwind': '$sessions'},
        {'$unwind': '$sessions.actions'},
        {
            '$group':
                {
                    '_id': {'number': '$number', 'type': '$sessions.actions.type'},
                    'count': {'$sum': 1},
                    'last': {'$max': '$sessions.actions.created_at'}
                }
        },
        {
            '$group':
                {
                    '_id': '$_id.number',
                    'actions':
                        {
                            '$push':
                                {
                                    'type': '$_id.type',
                                    'last': '$last',
                                    'count': '$count'
                                }
                        }
                }
        },
        {
            '$merge':
                {
                    'into': 'Result',

                }
        },

    ])

# Добавляем в Result информацию по действиям, которые пользователь не совершал
for actions_type in ['create', 'read', 'update', 'delete']:
    result.update_many(
        {
            'actions':
                {
                    '$not':
                        {
                            '$elemMatch':
                                {
                                    'type' : actions_type
                                }
                        }
                }
        },
        {
            '$addToSet':
                {
                    'actions': {'type': actions_type, 'last': '$null', 'count': 0}
                }

        },
 )

# Формируем итоговые данные
agg_res = result.aggregate([
    {
        '$project':
            {
                '_id': 0,
                'number': '$_id',
                'actions': '$actions'
            }
    },
])

for i in agg_res:
    print(i)



