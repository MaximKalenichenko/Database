import psycopg2
from abc import abstractmethod
from datetime import datetime


class Model:
    def __init__(self, database, user, password, host, port):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.returns = None
        self.dict_foo = {
            1: SelectTable,
            2: SelectAll,
            3: Search,
            4: InsertData,
            5: UpdateData,
            6: InsertRandom,
            7: DeleteData,
        }
        self.cursor = None

    def connect(self):
        return psycopg2.connect(dbname=self.database, user=self.user, password=self.password,
                                host=self.host, port=self.port)

    def execute(self, task, table=None, values=None):
        with self.connect() as con:
            try:
                self.cursor = con.cursor()
                task = self.dict_foo[task](self.cursor, table, values)
                self.returns = task()
                con.commit()
            except (Exception, psycopg2.Error) as e:
                print("Check you value", e)
            finally:
                if self.cursor:
                    self.cursor.close()
                    if self.returns:
                        return self.returns


class Task:
    def __init__(self, cursor, table, values):
        self.cur = cursor
        self.table = table
        self.values = values

    def print_fetchall(self):
        tuple_data = self.cur.fetchall()
        if tuple_data:
            for data in tuple_data:
                print(data)
            return tuple_data
        else:
            print('Table is empty')

    def create_random_str(self):
        uppercase_letter = "chr(ascii('A') + (random() * 25)::int)"
        lowercase_letter = "chr(ascii('a') + (random() * 25)::int)"
        self.cur.execute(f"""SELECT ({uppercase_letter}{(" || " + lowercase_letter) * 30})""")

    def create_random_int(self):
        self.cur.execute(f'''SELECT (random() * 100 + 1)::int AS RAND_1_11;''')

    def fetch_random_id(self, table, id_):
        self.cur.execute(f'''SELECT {id_} FROM "{table}" ORDER BY random() LIMIT(1)''')

    @staticmethod
    def str_for_insert(our_data):
        return "'" + str(our_data) + "'"

    @abstractmethod
    def __call__(self):
        raise NotImplemented


class SelectTable(Task):
    def __call__(self):
        self.cur.execute(f'''SELECT * FROM "{self.table}";''')
        self.print_fetchall()


class SelectAll(Task):
    def __call__(self):
        for table_name in self.values:
            self.cur.execute(f'''SELECT * FROM "{table_name}";''')
            print(table_name)
            self.print_fetchall()


class Search(Task):
    def __call__(self):
        self.cur.execute(f'''SELECT {self.values[0]} FROM "{self.table}" 
                        WHERE {self.values[1]}::VARCHAR LIKE '%{self.values[2]}%';''')
        self.print_fetchall()


class InsertData(Task):
    def __call__(self):
        str_keys = ''
        str_value = ''
        for key, value in self.values.items():
            str_keys += key + ' ,'
            str_value += self.str_for_insert(value) + ' ,' if isinstance(value, str) else str(value) + ', '
        str_keys = str_keys[:-2]
        str_value = str_value[:-2]
        self.cur.execute(f"""INSERT INTO "{self.table}" ({str_keys})
                                VALUES ({str_value}); """)
        print({'Request': 'completed'})


class UpdateData(Task):
    def __call__(self):
        column_id_ = list(self.values.keys())[0]
        id_ = self.values[column_id_]
        del self.values[column_id_]
        str_set = ''
        for key, value in self.values.items():
            if value:
                str_set += key + ' = ' + (self.str_for_insert(value) + ' ,'
                                          if isinstance(value, str) else str(value) + ' ,')
        str_set = str_set[:-2]
        self.cur.execute(f"""UPDATE  "{self.table}"
                                        SET {str_set}
                                        WHERE {column_id_}  = {id_}; """)
        print({'Request': 'completed'})


class InsertRandom(Task):
    def __call__(self):
        id_ = 1 if self.table != 'Goods_Client' else 0
        self.values = {}
        self.cur.execute(f'''SELECT column_name, data_type FROM information_schema.columns 
                        WHERE table_name = '{self.table}';''')
        tuple_data = [self.cur.fetchall()]
        self.cur.execute(f'''SELECT
                    tc.table_schema, 
                    tc.constraint_name, 
                    tc.table_name, 
                    kcu.column_name, 
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM 
                    information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN (select row_number() over (partition by table_schema, table_name, 
                        constraint_name order by row_num) ordinal_position,
                                 table_schema, table_name, column_name, constraint_name
                          from   (select row_number() over (order by 1) row_num, table_schema, 
                          table_name, column_name, constraint_name
                                  from   information_schema.constraint_column_usage
                                 ) t
                         ) AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                      AND ccu.ordinal_position = kcu.ordinal_position
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = '{self.table}';''')
        tuple_data.append(self.cur.fetchall())
        for list_data in tuple_data[0][id_:]:
            for data_column in tuple_data[1]:
                if list_data[0] == data_column[3]:
                    self.fetch_random_id(data_column[5], list_data[0])
                    break
            else:
                if list_data[1] == 'integer' or list_data[1] == 'numeric':
                    self.create_random_int()
                elif list_data[1] == 'date':
                    value = datetime.now().strftime('%Y-%m-%d')
                    self.values[list_data[0]] = value
                    continue
                else:
                    self.create_random_str()
            self.values[list_data[0]] = self.cur.fetchall()[0][0]
        return self.values


class DeleteData(Task):
    def __call__(self):
        self.cur.execute(f'''DELETE FROM "{self.table}" WHERE {self.values[0]} = {self.values[1]};''')
        print({'Request': 'completed'})