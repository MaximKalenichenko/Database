class View:
    def __init__(self, table, records):
        self.table = table
        self.records = records

    @staticmethod
    def complete_message(attribute, value, table, action):
        print(f"The row with '{attribute}' = '{value}' in table '{table}' was {action} successfully.")

    @staticmethod
    def enter_item(item):
        data = input("Enter {}: ".format(item))
        return data

    @staticmethod
    def list():
        print('''
            1 -> subject
            2 -> teacher
            3 -> student
            4 -> phone
            5 -> schedule
            ''')

    @staticmethod
    def columns(table):
        if table == 1:
            print('''
                1 -> name
            ''')
        elif table == 2:
            print('''
                1 -> firstname
                2 -> lastname
            ''')
        elif table == 3:
            print('''
                1 -> firstname
                2 -> lastname
            ''')
        elif table == 4:
            print('''
                1 -> phone number
                2 -> student ID
            ''')
        elif table == 5:
            print('''
                1 -> day
                2 -> time
                3 -> subject ID
                4 -> student ID
                5 -> teacher Id
            ''')