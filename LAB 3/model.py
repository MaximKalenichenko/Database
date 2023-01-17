from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Index, Column, Integer, String, Text, create_engine, Boolean, PrimaryKeyConstraint, \
    UniqueConstraint, ForeignKeyConstraint, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session, sessionmaker
import controller
from view import View
from numpy import array
from tabulate import tabulate
from pandas import DataFrame

tables = {
    1: 'subject',
    2: 'teacher',
    3: 'student',
    4: 'phone',
    5: 'schedule',
}

Base = declarative_base()
engine = create_engine('postgresql+psycopg2://postgres:1111@localhost:5432/school')


class Subject(Base):
    __tablename__ = 'subject'
    subject_id = Column(Integer, nullable=False, primary_key=True)
    name = Column(String, nullable=False)
    schedule = relationship('Schedule')

    def __init__(self, subject_id, name):
        self.subject_id = subject_id
        self.name = name

    def __repr__(self):
        return "{:>10}{:>35}".format(self.subject_id, self.name)


class Student(Base):
    __tablename__ = 'student'
    student_id = Column(Integer, primary_key=True)
    firstname = Column(String, nullable=False)
    lastname = Column(String, nullable=False)
    phones = relationship('Phone')
    schedule = relationship('Schedule')

    def __init__(self, student_id, firstname, lastname):
        self.student_id = student_id
        self.firstname = firstname
        self.lastname = lastname

    def __repr__(self):
        return "{:>10}{:>35}{:>35}".format(self.student_id, self.firstname, self.lastname)


class Teacher(Base):
    __tablename__ = 'teacher'
    teacher_id = Column(Integer, primary_key=True)
    firstname = Column(String, nullable=False)
    lastname = Column(String, nullable=False)
    schedule = relationship('Schedule')

    def __init__(self, teacher_id, firstname, lastname):
        self.teacher_id = teacher_id
        self.firstname = firstname
        self.lastname = lastname

    def __repr__(self):
        return "{:>10}{:>35}{:>35}".format(self.teacher_id, self.firstname, self.lastname)


class Phone(Base):
    __tablename__ = 'phone'
    phone_id = Column(Integer, primary_key=True)
    phonenumber = Column(String, nullable=False)
    studentphone_fk = Column(Integer, ForeignKey('student.student_id'))

    def __init__(self, phone_id, phonenumber, studentphone_fk):
        self.phone_id = phone_id
        self.phonenumber = phonenumber
        self.studentphone_fk = studentphone_fk

    def __repr__(self):
        return "{:>10}{:>35}{:>10}".format(self.phone_id, self.phonenumber, self.studentphone_fk)


class Schedule(Base):
    __tablename__ = 'schedule'
    schedule_id = Column(Integer, primary_key=True)
    day = Column(String, nullable=False)
    time = Column(String, nullable=False)
    subject_fk = Column(Integer, ForeignKey('subject.subject_id'))
    student_fk = Column(Integer, ForeignKey('student.student_id'))
    teacher_fk = Column(Integer, ForeignKey('teacher.teacher_id'))

    def __init__(self, schedule_id, day, time, subject_fk, student_fk, teacher_fk):
        self.schedule_id = schedule_id
        self.day = day
        self.time = time
        self.subject_fk = subject_fk
        self.student_fk = student_fk
        self.teacher_fk = teacher_fk

    def __repr__(self):
        return "{:>10}{:>35}{:>35}{:>10}{:>10}{:>10}".format(self.schedule_id, self.day, self.time, self.subject_fk,
                                                             self.student_fk, self.teacher_fk)


def display_query(rows, headers):
    df = DataFrame([array(el) for el in rows], columns=array(headers))
    print(tabulate(df, headers="keys", tablefmt="pretty", showindex=False))


def show_subject():
    print("\033[1m Subject \033[0m")
    Session = sessionmaker(bind=engine)
    session = Session()
    records = session.query(Subject.subject_id, Subject.name).all()
    return records


def show_teacher():
    print("\033[1m Teacher \033[0m")
    Session = sessionmaker(bind=engine)
    session = Session()
    records = session.query(Teacher.teacher_id, Teacher.firstname, Teacher.lastname).all()
    return records


def show_student():
    print("\033[1m Student \033[0m")
    Session = sessionmaker(bind=engine)
    session = Session()
    records = session.query(Student.student_id, Student.firstname, Student.lastname).all()
    return records


def show_phone():
    print("\033[1m Phone \033[0m")
    Session = sessionmaker(bind=engine)
    session = Session()
    records = session.query(Phone.phone_id, Phone.phonenumber, Phone.studentphone_fk).all()
    return records


def show_schedule():
    print("\033[1m Schedule \033[0m")
    Session = sessionmaker(bind=engine)
    session = Session()
    records = session.query(Schedule.schedule_id, Schedule.day, Schedule.time, Schedule.student_fk,
                            Schedule.teacher_fk, Schedule.subject_fk).all()
    return records


def insert() -> None:
    Session = sessionmaker(bind=engine)
    session = Session()
    go_on = True
    while go_on:
        View.list()
        table = controller.validtable()
        if table == 1:
            id = controller.validate_input_items("subject_id")
            name = controller.validate_input_items("name")
            s = Subject(subject_id=id, name=name)
            session.add(s)
            View.complete_message("subject_id", id, "subject", "inserted")
            go_on = False
        elif table == 2:
            id = controller.validate_input_items("teacher_id")
            firstname = controller.validate_input_items("firstname")
            lastname = controller.validate_input_items("lastname")
            s = Teacher(teacher_id=id, firstname=firstname, lastname=lastname)
            session.add(s)
            View.complete_message("teacher_id", id, "teacher", "inserted")
            go_on = False
        elif table == 3:
            id = controller.validate_input_items("student_id")
            firstname = controller.validate_input_items("firstname")
            lastname = controller.validate_input_items("lastname")
            s = Student(student_id=id, firstname=firstname, lastname=lastname)
            session.add(s)
            View.complete_message("student_id", id, "student", "inserted")
            go_on = False
        elif table == 4:
            id = controller.validate_input_items("phone_id")
            phonenumber = controller.validate_input_items("phonenumber")
            studentphone_fk = controller.validate_input_items("student_id")
            s = Phone(phone_id=id, phonenumber=phonenumber, studentphone_fk=studentphone_fk)
            session.add(s)
            View.complete_message("phone_id", id, "phone", "inserted")
            go_on = False
        elif table == 5:
            id = controller.validate_input_items("schedule_id")
            day = controller.validate_input_items("day")
            time = controller.validate_input_items("time")
            subject_fk = controller.validate_input_items("subject_id")
            student_fk = controller.validate_input_items("student_id")
            teacher_fk = controller.validate_input_items("teacher_id")
            s = Schedule(schedule_id=id, day=day, time=time, subject_fk=subject_fk, student_fk=student_fk,
                         teacher_fk=teacher_fk)
            session.add(s)
            View.complete_message("schedule_id", id, "schedule", "inserted")
            go_on = False
        session.commit()


def delete():
    Session = sessionmaker(bind=engine)
    session = Session()
    go_on = True
    while go_on:
        View.list()
        table = controller.validtable()
        if table == 1:
            id = controller.validate_input_items("subject_id")
            records = session.query(Subject).get(id)
            if records is not None:
                records = session.query(Schedule).get(id)
                if records is not None:
                    delete = session.query(Schedule).filter(Schedule.subject_fk == id)
                    for i in delete:
                        session.delete(i)
                    View.complete_message("subject_fk", id, "schedule", "deleted")
                session.delete(session.query(Subject).filter(Subject.subject_id == id).one())
                View.complete_message("subject_id", id, "subject", "deleted")
            else:
                controller.message("No ID found")
            go_on = False
        elif table == 2:
            id = controller.validate_input_items("teacher_id")
            records = session.query(Teacher).get(id)
            if records is not None:
                records = session.query(Schedule).get(id)
                if records is not None:
                    delete = session.query(Schedule).filter(Schedule.teacher_fk == id)
                    for i in delete:
                        session.delete(i)
                    View.complete_message("teacher_fk", id, "schedule", "deleted")
                session.delete(session.query(Teacher).filter(Teacher.teacher_id == id).one())
                View.complete_message("teacher_id", id, "teacher", "deleted")
            else:
                controller.message("No ID found")
            go_on = False
        elif table == 3:
            id = controller.validate_input_items("student_id")
            records = session.query(Student).get(id)
            if records is not None:
                records = session.query(Phone).get(id)
                if records is not None:
                    delete = session.query(Schedule).filter(Schedule.studentphone_fk == id)
                    for i in delete:
                        session.delete(i)
                    View.complete_message("studentphone_fk", id, "phone", "deleted")
                records = session.query(Schedule).get(id)
                if records is not None:
                    delete = session.query(Schedule).filter(Schedule.student_fk == id)
                    for i in delete:
                        session.delete(i)
                    View.complete_message("student_fk", id, "schedule", "deleted")
                session.delete(session.query(Student).filter(Student.student_id == id).one())
                View.complete_message("student_id", id, "student", "deleted")
            else:
                controller.message("No ID found")
            go_on = False
        elif table == 4:
            id = controller.validate_input_items("phone_id")
            records = session.query(Phone).get(id)
            if records is not None:
                session.delete(session.query(Phone).filter(Phone.phone_id == id).one())
                View.complete_message("phone_id", id, "phone", "deleted")
            else:
                controller.message("No ID found")
            go_on = False
        elif table == 5:
            id = controller.validate_input_items("schedule_id")
            records = session.query(Schedule).get(id)
            if records is not None:
                session.delete(session.query(Schedule).filter(Schedule.schedule_id == id).one())
                View.complete_message("schedule_id", id, "schedule", "deleted")
            else:
                controller.message("No ID found")
            go_on = False
        else:
            "Input correct number"
        session.commit()
        pass


def update():
    Session = sessionmaker(bind=engine)
    session = Session()
    go_on = True
    while go_on:
        View.list()
        table = controller.validtable()
        if table == 1:
            id = controller.validate_input_items("subject_id")
            records = session.query(Subject).get(id)
            if records is not None:
                value = controller.validate_input_items("name")
                upd = session.query(Subject).get(id)
                upd.name = value
                session.add(upd)
            else:
                print("No subject with this ID")
            go_on = False
        elif table == 2:
            id = controller.validate_input_items("teacher_id")
            records = session.query(Teacher).get(id)
            if records is not None:
                View.columns(2)
                continue_update = True
                while continue_update:
                    attr = input("Choose a number of column to update ")
                    if attr == '1':
                        value = controller.validate_input_items("firstname")
                        upd = session.query(Teacher).get(id)
                        upd.firstname = value
                        session.add(upd)
                        continue_update = False
                    elif attr == '2':
                        value = controller.validate_input_items("lastname")
                        upd = session.query(Teacher).get(id)
                        upd.lastname = value
                        session.add(upd)
                        continue_update = False
                    else:
                        print("Enter correct number ")
                View.complete_message("teacher_id", id, "teacher", "updated")
                go_on = False
                pass
            else:
                print("No teacher with this ID")
        elif table == 3:
            id = controller.validate_input_items("student_id")
            records = session.query(Student).get(id)
            if records is not None:
                View.columns(3)
                continue_update = True
                while continue_update:
                    attr = input("Choose a number of column to update: ")
                    if attr == '1':
                        value = controller.validate_input_items("firstname")
                        upd = session.query(Student).get(id)
                        upd.firstname = value
                        session.add(upd)
                        continue_update = False
                    elif attr == '2':
                        value = controller.validate_input_items("lastname")
                        upd = session.query(Student).get(id)
                        upd.lastname = value
                        session.add(upd)
                        continue_update = False
                    else:
                        print("Enter correct number ")
                    View.complete_message("student_id", id, "student", "updated")
                    go_on = False
                    pass
            else:
                print("No student with this ID")
        elif table == 4:
            id = controller.validate_input_items("phone_id")
            records = session.query(Phone).get(id)
            if records is not None:
                View.columns(4)
                continue_update = True
                while continue_update:
                    attr = input("Choose a number of column to update: ")
                    if attr == '1':
                        value = controller.validate_input_items("phonenumber")
                        upd = session.query(Phone).get(id)
                        upd.phonenumber = value
                        session.add(upd)
                        continue_update = False
                    elif attr == '2':
                        value = controller.validate_input_items("student_id")
                        upd = session.query(Phone).get(id)
                        upd.studentphone_fk = value
                        session.add(upd)
                        continue_update = False
                    go_on = False
                    pass
            else:
                print("No phone with this ID")
        elif table == 5:
            id = controller.validate_input_items("schedule_id")
            records = session.query(Schedule).get(id)
            if records is not None:
                View.columns(5)
                continue_update = True
                while continue_update:
                    attr = input("Choose a number of column to update: ")
                    if attr == '1':
                        value = controller.validate_input_items("day")
                        upd = session.query(Schedule).get(id)
                        upd.day = value
                        session.add(upd)
                        continue_update = False
                    elif attr == '2':
                        value = controller.validate_input_items("time")
                        upd = session.query(Schedule).get(id)
                        upd.time = value
                        session.add(upd)
                        continue_update = False
                    elif table == '3':
                        value = controller.validate_input_items("subject_id")
                        upd = session.query(Schedule).get(id)
                        upd.subject_fk = value
                        session.add(upd)
                        continue_update = False
                    elif table == '4':
                        value = controller.validate_input_items("student_id")
                        upd = session.query(Schedule).get(id)
                        upd.student_fk = value
                        session.add(upd)
                        continue_update = False
                    elif table == '5':
                        value = controller.validate_input_items("teacher_id")
                        upd = session.query(Schedule).get(id)
                        upd.teacher_fk = value
                        session.add(upd)
                        continue_update = False
                go_on = False
                pass
            else:
                print("No schedule with this ID")
        else:
            print("Please enter correct number ")
    session.commit()
    pass