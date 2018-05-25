import sqlite3
import csv
import os
import sys
import fnmatch


# this is needed due to some values being too laarge for default size
def setMaxFiledLimit():
    maxInt = sys.maxsize
    decrement = True

    while decrement:
        # decrease the maxInt value by factor 10
        # as long as the OverflowError occurs.

        decrement = False
        try:
            csv.field_size_limit(maxInt)
        except OverflowError:
            maxInt = int(maxInt / 10)
            decrement = True


# takes a table name and a list of parameters
# Will drop the table if it exists
def createTable(table_name, headers):
    cursor.execute('''drop table if exists ''' + table_name)

    cursor.execute('''CREATE TABLE ''' + table_name + '''(''' + ','.join(headers) + ''')''')


def createRequestUserOptionsJoinTable(table_name):
    cursor.execute('''drop table if exists ''' + table_name)
    column_names = 'RowId TEXT PRIMARY KEY,  RequestRowId, RequestUserOptionsRowId, ' \
                   'FOREIGN KEY (RequestRowId) REFERENCES Request(RowId) ' \
                   'FOREIGN KEY (RequestUserOptionsRowId) REFERENCES (RequestUserOptions)'
    cursor.execute('''CREATE TABLE ''' + table_name + '''(''' + column_names + ''')''')
    print('User Options Junction table made')


def createUserOptionsHeader():
    path = "C:\\Users\\jdickey\\Desktop\\SN imports\\"
    pattern = 'Request_UserOption*'
    header = []

    for root, dirs, files in os.walk(path):
        for file in fnmatch.filter(files, pattern):
            reader = csv.reader(open(root + '\\' + file, 'r', encoding="utf8"), delimiter=',', quotechar='"')
            counter = 0

            for row in reader:
                counter += 1
                if counter == 1:
                    for item in row:
                        if item not in header:
                            header.append(item)

    print('User Options column headers  made')
    return header


# takes a foldername and a table name as parameters
# goes the the folder, reads in every .csv file within the folder and dumps the data in the table
def readAllcsv(folder_name, table_name):
    dir = os.path.join("C:\\", "Users\\jdickey\\Desktop\\SN imports\\" + folder_name)
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.endswith(".csv"):
                f = open(dir + '\\' + file, 'r', encoding="utf8")
                reader = csv.reader(f, delimiter=',', quotechar='"')
                counter = 0

                for row in reader:
                    counter += 1
                    if counter == 1:
                        header = row
                        continue
                    counter_col = 0
                    values = ''

                    for item in row:
                        values += '?,'
                        counter_col += 1

                    values = values[:-1]
                    cursor.execute('''INSERT into ''' + table_name + ''' VALUES (''' + values + ''')''', row)
                f.close()


# takes in a foldername as a parameter
# graps the first line in the first .csv file and makes that the column headers in the table.
def getheaders(folder_name):

    if folder_name == 'RequestUserOptions' or folder_name == 'UserOption':
        return

    dir = os.path.join("C:\\", "Users\\jdickey\\Desktop\\SN imports\\" + folder_name)
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.endswith(".csv"):
                reader = csv.reader(open(dir + '\\' + file, 'r', encoding="utf8"), delimiter=',', quotechar='"')
                counter = 0

                for row in reader:
                    counter += 1
                    if counter == 1:
                        return row

                break


setMaxFiledLimit()

conn = sqlite3.connect('SNImport.db')
cursor = conn.cursor()

# lists of table names and foreign keys as needed
table_names = ['Person', 'PersonGroup', 'ServiceDefinition', 'Category', 'Article', 'Change', 'Incident', 'Request',
               'UserOptions']
person_foreign_keys = ['OwnedByPerson', 'AssignedPerson', 'RecordedByPerson', 'RequestedByPerson', 'LastUpdateByPerson',
                       'CreatedByPerson', 'ClosedByPerson', 'ContactPerson', 'SolvedByPerson', 'CompOpsExpertOnCall_c',
                       'CompOpsOnCall_c']
person_group_foreign_keys = ['KMAssignedGroup_c', 'OwnedByGroup' 'CompOpsGroup_c', 'AssignedGroup']
service_foreign_keys = ['Service']

# iterates the list of tablenames, creates and loads the tables with data
for item in table_names:
    if item == 'UserOptions':
        header = createUserOptionsHeader()
        header = ['RowId PRIMARY KEY' if x == 'RowId' else x for x in header]
        header = ['Index_c' if x == 'Index' else x for x in header]

    else:
        header = getheaders(item)
        # sets the 'Id' column as the PRIMARY KEY
        header = ['Id INTEGER PRIMARY KEY' if x == 'Id' else x for x in header]

    # adds the FOREIGN KEY constrainds for any columns in the related list
    for key in person_foreign_keys:
        if key in header:
            header.append('FOREIGN KEY (' + key + ') REFERENCES Person(Id)')
    # adds the FOREIGN KEY constrainds for any columns in the related list
    for key in service_foreign_keys:
        if key in header:
            header.append('FOREIGN KEY (' + key + ') REFERENCES Service(Id)')

    # adds the FOREIGN KEY constraints for any columns in the related list
    for key in person_group_foreign_keys:
        if key in header:
            header.append('FOREIGN KEY (' + key + ') REFERENCES PersonGroup(Id)')

    createTable(item, header)
    readAllcsv(item, item)
    #TODO load in userOptions data from each user optoin file.
    print('Table: ' + item + ' had bean created and loaded with data')


conn.commit()

print("database has been loaded")
