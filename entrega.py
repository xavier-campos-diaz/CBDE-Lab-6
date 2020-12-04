import pymongo
import numpy as np
from pymongo import MongoClient
import string
import random
from datetime import datetime
client = MongoClient('mongodb://localhost:27017/')
dblists = client.list_database_names()
db = client["TPC-H"]
collection = db["LineItem"]

def printOptions():
    print("Enter 1 to execute query 1")
    print("Enter 2 to execute query 2")
    print("Enter 3 to execute query 3")
    print("Enter 4 to execute query 4")
    print("Enter -1 to exit this program")
    return int(input("What do you want to do? "))

def initializeDB():
    print("Initializing DB!")
    posts = []
    for i in range(200):
        order_key = np.random.randint(1, 20)
        quantity = float(np.random.randint(1, 5))
        extendedprice = float(np.random.randint(5, 1500) + np.random.randint(0, 99)/100)
        discount = float(np.random.randint(0, 65)/100)
        tax = float(random.choice([4,10,21])/100)
        shipdate = datetime(np.random.randint(2000, 2020), np.random.randint(1, 12), np.random.randint(1, 30))
        returnflag = random.choice(string.ascii_letters)
        linestatus = random.choice(string.ascii_letters)
        posts.append({"_id":i, "order_key": order_key, "quantity": quantity, "extendedprice": extendedprice, 
                    "discount": discount, "tax": tax, "shipdate": shipdate, "returnflag": returnflag, "linestatus": linestatus})

    collection.insert_many(posts)

def execute_q1(date):
    results = collection.aggregate( [
        {"$match": {
            "shipdate": {"$gt": date }
        }},
        {"$project": {
            "_id": 0,
            "returnflag": 1,
            "linestatus": 1,
            "sum_qty": {"$sum": "$quantity"},
            "sum_base_price": {"$sum": "$extendedprice"},
            "sum_disc_price": { "$multiply": [ {"$sum": "$extendedprice"}, {"$subtract": [1, "$discount"] }]},
            "sum_charge": { "$multiply": [ { "$multiply": [ {"$sum": "$extendedprice"}, {"$subtract": [1, "$discount"] }]}, {"$sum": [1, "$tax"]}]},
            "avg_qty": { "$avg": "$quantity"},
            "avg_price": { "$avg": "$extendedprice"},
            "avg_disc": { "$avg": "$discount"}
        }},
        {"$group": {
            "_id" : { "returnflag": "$returnflag", "linestatus": "$linestatus" },   
        }},
        {"$sort": {
            "_id.returnflag":1, "_id.linestatus":1
        }}
    ])
    for x in results:
        print(x)


def execute_q2():
    print("Not yet implemented")

def execute_q3():
    print("Not yet implemented")

def execute_q4():
    print("Not yet implemented")

if __name__ == "__main__":
    if ("TPC-H" not in dblists):
        initializeDB() 
    value = printOptions()
    while (value != -1):
        if (value == 1):
            year = int(input("Enter the year "))
            month = int(input("Enter the month "))
            day = int(input("Enter the day "))
            execute_q1(datetime(year, month, day))
        elif (value == 2):
            execute_q2()
        elif (value == 3):
            execute_q3()
        elif (value == 4):
            min_age = int(input("Minimum age "))
            max_age = int(input("Maximum age "))
            execute_q4()
        print("\n")
        value = printOptions()
    print("Goodbye!")