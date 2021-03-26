import sqlite3
import pandas as pd

conn = sqlite3.connect('./Student.sqlite')
my_cursor = conn.cursor()


def CreateStudentTable():
    my_cursor.execute('''
    CREATE TABLE students(
    StudentID INTEGER PRIMARY KEY AUTOINCREMENT ,
    FirstName TEXT,
    LastName TEXT,
    GPA REAL,
    Major TEXT,
    FacultyAdvisor TEXT,
    Address TEXT,
    City TEXT,
    State TEXT,
    ZipCode Text,
    MobilePhoneNumber TEXT,
    isDeleted INTEGER DEFAULT 0
);
''')


def DeleteTable():
    my_cursor.execute("drop table students")


def ReadCSV():
    student_data = pd.read_csv('./students.csv')
    student_data.to_sql('students', conn, if_exists='append', index=False)


def Display():
    my_cursor.execute('SELECT * FROM students')
    my_records = my_cursor.fetchall()
    df = pd.DataFrame(my_records, columns=['StudentID', 'FirstName', 'LastName', 'GPA', 'Major', 'FacultyAdvisor',
                                           'Address', 'City', 'State', 'ZipCode', 'MobilePhoneNumber', 'isDeleted'])
    print(df.to_string())
    del df


def AddStudent():
    new_student = []
    print("Enter Fields: ")
    fn = input("First Name: ")
    ln = input("Last Name: ")
    gpa = input("GPA: ")
    major = input("Major: ")
    advisor = input("Advisor : ")
    address = input("Street Address : ")
    city = input("City : ")
    state = input("State : ")
    zip = input("Zip Code : ")
    phone = input("Phone Number : ")
    new_student.append((fn, ln, gpa, major, advisor, address, city, state, zip, phone))
    conn.executemany("INSERT INTO students(FirstName, LastName, GPA, Major, FacultyAdvisor, Address, City, "
                     "State, ZipCode, MobilePhoneNumber) VALUES(?,?,?,?,?,?,?,?,?,?)", new_student)
    print("Student added to table")
    conn.commit()


def UpdateStudent():
    print("Choose Attribute to update")
    print("Major,FacultyAdvisor,MobilePhoneNumber")
    while True:
        choice = input(":")
        if choice.upper() == "MAJOR":
            student_id = input("\nStudent Id:")
            major = input("New Major : ")
            my_cursor.execute("UPDATE students SET Major = ? WHERE StudentID = ?", (major, student_id,))
            print("Done")
            break
        elif choice.upper() == "FACULTYADVISOR":
            student_id = input("\nStudent Id: ")
            advisor = input("New Advisor : ")
            my_cursor.execute("UPDATE students SET FacultyAdvisor = ? WHERE StudentID = ?", (advisor, student_id,))
            print("Done")
            break
        elif choice.upper() == "MOBILEPHONENUMBER":
            student_id = input("\nStudent Id: ")
            phone = input("New Phone Number : ")
            my_cursor.execute("UPDATE students SET MobilePhoneNumber = ? WHERE StudentID = ?", (phone, student_id,))
            print("Done")
            break
        else:
            print("Invalid Input, try again:")
            print("Major,FacultyAdvisor,MobilePhoneNumber")
    conn.commit()


def GetAttribute():
    attribute = []
    print("Select Attribute")
    print("Major,GPA,City,State,FacultyAdvisor")
    while True:
        choice = input(":")
        if choice.upper() == "MAJOR":
            major = input("Enter Major: ")
            attribute.append(major)
            my_cursor.execute('SELECT * FROM students WHERE Major = ?', attribute)
            my_records = my_cursor.fetchall()
            df = pd.DataFrame(my_records, columns=['StudentID', 'FirstName', 'LastName', 'GPA', 'Major', 'FacultyAdvisor',
                                                   'Address', 'City', 'State', 'ZipCode', 'MobilePhoneNumber', 'isDeleted'])
            print(df.to_string())
            del df
            break
        elif choice.upper() == "GPA":
            gpa = input("Enter GPA: ")
            attribute.append(gpa)
            my_cursor.execute('SELECT * FROM students WHERE GPA = ?', attribute)
            my_records = my_cursor.fetchall()
            df = pd.DataFrame(my_records, columns=['StudentID', 'FirstName', 'LastName', 'GPA', 'Major', 'FacultyAdvisor',
                                                   'Address', 'City', 'State', 'ZipCode', 'MobilePhoneNumber', 'isDeleted'])
            print(df.to_string())
            del df
            break
        elif choice.upper() == "CITY":
            city = input("Enter City: ")
            attribute.append(city)
            my_cursor.execute('SELECT * FROM students WHERE City = ?', attribute)
            my_records = my_cursor.fetchall()
            df = pd.DataFrame(my_records, columns=['StudentID', 'FirstName', 'LastName', 'GPA', 'Major', 'FacultyAdvisor',
                                                   'Address', 'City', 'State', 'ZipCode', 'MobilePhoneNumber', 'isDeleted'])
            print(df.to_string())
            del df
            break
        elif choice.upper() == "STATE":
            state = input("Enter State: ")
            attribute.append(state)
            my_cursor.execute('SELECT * FROM students WHERE State = ?', attribute)
            my_records = my_cursor.fetchall()
            df = pd.DataFrame(my_records, columns=['StudentID', 'FirstName', 'LastName', 'GPA', 'Major', 'FacultyAdvisor',
                                                   'Address', 'City', 'State', 'ZipCode', 'MobilePhoneNumber', 'isDeleted'])
            print(df.to_string())
            del df
            break
        elif choice.upper() == "FACULTYADVISOR":
            advisor = input("Enter Advisor: ")
            attribute.append(advisor)
            my_cursor.execute('SELECT * FROM students WHERE FacultyAdvisor = ?', attribute)
            my_records = my_cursor.fetchall()
            df = pd.DataFrame(my_records, columns=['StudentID', 'FirstName', 'LastName', 'GPA', 'Major', 'FacultyAdvisor',
                                                   'Address', 'City', 'State', 'ZipCode', 'MobilePhoneNumber', 'isDeleted'])
            print(df.to_string())
            del df
            break
        else:
            print("Invalid Input, try again:")
            print("Major,GPA,City,State,FacultyAdvisor")


def Delete():
    id = input("Enter StudentID value for Student to be deleted : ")
    my_cursor.execute("UPDATE students SET isDeleted = 1 WHERE StudentID = ?", [id])
    conn.commit()
    print("Student: " + str(id) + " has been Deleted")


def DisplayPrompt():
    print("\n1: Import Data to Table")
    print("2: Add Student")
    print("3: Update Student")
    print("4: Delete Student")
    print("5: Search Table")
    print("6: Display all Data")
    print("7: Exit\n")


def main():
    print("If you create the table during this program, and do not proceed to exit by entering option 7")
    print("There will be an error, please make sure to exit using option 7, otherwise you will need to ")
    print("Manually delete the sqlite file created in the directory")
    print("Also please enter just a number while choosing an option, entering a string will cause an error")

    CreateStudentTable()
    isImported = False;
    while True:
        DisplayPrompt()
        x = int(input(": "))
        if x == 1:
            ReadCSV()
            isImported = True;
        elif not isImported:
            print("You must import the data from the csv before running any queries")
            print("Please choose option 1 before continuing")
        elif x == 2:
            AddStudent()
        elif x == 3:
            UpdateStudent()
        elif x == 4:
            Delete()
        elif x == 5:
            GetAttribute()
        elif x == 6:
            Display()
        elif x == 7:
            DeleteTable()
            break;
        else:
            print("Not a valid choice, try again: \n")
    print("Deleting Student table")
    print("Exiting")


main()
conn.close()