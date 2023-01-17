from controller import Controller

def main():
    dict_table = {
        'Cafe': ['cafe_id', 'name', 'address'],
        'Goods': ['goods_id', 'name', 'price', 'cafe_id'],
        'Client': ['client_id', 'name'],
        'Goods_Client': ['goods_id', 'client_id'],
    }
    dbname = 'Cafe'
    user = 'postgres'
    password = '1234'
    host = 'localhost'
    port = '5432'
    control = Controller(dict_table, dbname, user, password, host, port)
    control.menu()

if __name__ == '__main__':
    main(