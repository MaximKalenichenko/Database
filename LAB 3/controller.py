from view import View


def message(text):
    return print(text)


def validtable():
    incorrect = True
    while incorrect:
        table = input('Choose table number => ')
        if table.isdigit():
            table = int(table)
            if 1 <= table <= 5:
                incorrect = False
            else:
                print('Incorrect input, try again.')
        else:
            print('Incorrect input, try again.')
    return table


def validate_input_items(name):
    if name == "id":
        value = View.enter_item("id")
        if value.isdecimal():
            return value
    elif name == "subject_id":
        value = View.enter_item("subject_id")
        if value.isdecimal():
            return value
    elif name == "teacher_id":
        value = View.enter_item("teacher_id")
        if value.isdecimal():
            return value
        else:
            message("enter only number")
            validate_input_items("teacher_id")
    elif name == "phone_id":
        value = View.enter_item("phone_id")
        if value.isdecimal():
            return value
    elif name == "schedule_id":
        value = View.enter_item("schedule_id")
        if value.isdecimal():
            return value
    elif name == "student_id":
        value = View.enter_item("student_id")
        if value.isdecimal():
            return value
    elif name == "name":
        value = View.enter_item("name")
        if value.isalpha() is False:
            return validate_input_items(name)
        return value
    elif name == "firstname":
        value = View.enter_item("firstname")
        if value.isalpha() is False:
            return validate_input_items(name)
        return value
    elif name == "lastname":
        value = View.enter_item("lastname")
        if value.isalpha() is False:
            return validate_input_items(name)
        return value
    elif name == "phonenumber":
        value = View.enter_item("phone_number")
        if value.isdigit() is False:
            print("Enter like in example: 0998889999")
            return validate_input_items(name)
        else:
            if len(value) != 10:
                print("Phone should have 10 numbers")
                return validate_input_items(name)
            return str("+38" + value)
    elif name == "day":
        value = View.enter_item("day")
        if value.isalpha() is False:
            return validate_input_items(name)
        return value
    elif name == "time":
        value = View.enter_item("time")
        li = list(value.split(":"))
        li_hour = li[0]
        li_min = li[1]
        if li_hour.isdecimal() and li_min.isdecimal():
            if 20 > int(li_hour) > 7 and -1 < int(li_min) < 60:
                li = [li_hour, li_min]
                return ':'.join(li)
            else:
                message("Please, enter hours from 7 to 20 and minutes from 0 to 59")
                return validate_input_items(name)
        else:
            message("Please, enter only digits with : between")