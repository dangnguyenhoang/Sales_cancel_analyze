import mysql.connector as myconnect



mydb = myconnect.connect(
  host="localhost",
  user="root",
  password="12345678"
)

print(mydb)
cursor=mydb.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS CEL")
cursor.execute(" SHOW DATABASES")
for x in cursor:
    print(x)