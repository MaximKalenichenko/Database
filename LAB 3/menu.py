import model
from view import View
import controller


class Menu:
    @staticmethod
    def menu():
        while True:
            print('''
                Main menu
                0 - Show one table
                1 - Show all tables
                2 - Insert data
                3 - Delete data
                4 - Update data
                5 - Exit
                ''')
            choice = input('Choose an option: ')
            if choice == '0':
                View.list()
                table = controller.validtable()
                if table == 1:
                    model.display_query(model.show_subject(), ["subject_id", "name"])
                elif table == 2:
                    model.display_query(model.show_teacher(), ["teacher_id", "firstname", "lastname"])
                elif table == 3:
                    model.display_query(model.show_student(), ["student_id", "firstname", "lastname"])
                elif table == 4:
                    model.display_query(model.show_phone(), ["phone_id", "phonenumber", "studentphone_fk"])
                elif table == 5:
                    model.display_query(model.show_schedule(),
                                        ["schedule_id", "day", "time", "student_fk", "teacher_fk", "subject_fk"])
            elif choice == '1':
                model.display_query(model.show_subject(), ["subject_id", "name"])
                model.display_query(model.show_teacher(), ["teacher_id", "firstname", "lastname"])
                model.display_query(model.show_student(), ["student_id", "firstname", "lastname"])
                model.display_query(model.show_phone(), ["phone_id", "phonenumber", "studentphone_fk"])
                model.display_query(model.show_schedule(),
                                    ["schedule_id", "day", "time", "student_fk", "teacher_fk", "subject_fk"])
            elif choice == '2':
                end_insert = False
                while not end_insert:
                    model.insert()
                    incorrect = True
                    while incorrect:
                        answer = input('Continue working with insert? Enter Yes or No ')
                        if answer == 'No':
                            end_insert = True
                            incorrect = False
                        elif answer == 'Yes':
                            incorrect = False
                            pass
                        else:
                            print('Please, enter Yes or No')
            elif choice == '3':
                end_delete = False
                while not end_delete:
                    model.delete()
                    incorrect = True
                    while incorrect:
                        answer = input('Continue working with delete? Enter Yes or No ')
                        if answer == 'No':
                            end_delete = True
                            incorrect = False
                        elif answer == 'Yes':
                            incorrect = False
                            pass
                        else:
                            print('Please, enter Yes or No ')
            elif choice == '4':
                end_update = False
                while not end_update:
                    model.update()
                    incorrect = True
                    while incorrect:
                        answer = input('Continue working with update? Enter Yes or No ')
                        if answer == 'No':
                            end_update = True
                            incorrect = False
                        elif answer == 'Yes':
                            incorrect = False
                            pass
                        else:
                            print('Please, enter Yes or No ')
            elif choice == '5':
                break
            else:
                print('Please, enter valid number')