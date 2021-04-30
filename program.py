import csv
import sqlite3


conn = sqlite3.connect('Hospital.db') #creates a database hospital
c = conn.cursor()
file1 = open('file.txt', 'r')
Lines = file1.readlines()

for line in Lines:
    stripped = (line.strip() for line in Lines)
    lines = (line.split("|") for line in stripped if line)
    with open('Intermediate.csv', 'w') as out_file:
        writer = csv.writer(out_file)
        writer.writerows(lines)

c.execute("""CREATE TABLE StagingTable
                 (Customer_Name,Customer_Id,Open_Date,Last_Consulted_Date,Vaccination_Id,Dr_Name,State,Country,DOB,Is_Active)""") #creates staging table
 
count=[] 
with open('Intermediate.csv') as csvfile:
    readCSV = csv.reader(csvfile)
    for row in readCSV:
        if row:
            country=row[9]
            #This block of code will create tables according to country different country data have different table
            if country not in count:
                c.execute("""CREATE TABLE %s (Customer_Name,Customer_Id,Open_Date,Last_Consulted_Date,Vaccination_Id,Dr_Name,State,Country,DOB,Is_Active)"""%country)
                c.execute('INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?)'%country,[row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11]])
                count.append(country)         
            else:
                c.execute('INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?)'%country,[row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11]])
conn.commit()


with open('Intermediate.csv') as csvfile:
    readCSV = csv.reader(csvfile)
    for row in readCSV:
        if row:
            c.execute('INSERT INTO StagingTable VALUES (?,?,?,?,?,?,?,?,?,?)',[row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11]])
conn.commit()          



#THIS ALL WILL PRINT DATA FROM RESPECTIVE COUNTRY TABLE 
sqls = "SELECT * FROM IND"
recss = c.execute(sqls)
print("DATA FROM INDIA")
if True:
    for row in recss:
        print(row)
print()

sqls = "SELECT * FROM USA"
recss = c.execute(sqls)
print("DATA FROM USA")
if True:
    for row in recss:
        print(row)
print()

sqls = "SELECT * FROM AU"
recss = c.execute(sqls)
print("DATA FROM AU")
if True:
    for row in recss:
        print(row)

c.close()  
 

  