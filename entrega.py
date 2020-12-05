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
    collection = db["LineItem"]
    for i in range(200):
        order_key = np.random.randint(1, 20)
        order_date = datetime(np.random.randint(2000, 2020), np.random.randint(1, 12), np.random.randint(1, 27))
        order_shippriority = random.choice(string.ascii_uppercase)*15
        customer_mkt_seg = random.choice(string.ascii_uppercase)*10
        customer_nation_key = np.random.randint(1, 25)
        order = {"_id": order_key, "order_date": order_date, "shippriority": order_shippriority, "c_mktsegment": customer_mkt_seg, "c_nationkey": customer_nation_key}
        quantity = float(np.random.randint(1, 5))
        extendedprice = float(np.random.randint(5, 1500) + np.random.randint(0, 99)/100)
        discount = float(np.random.randint(0, 65)/100)
        tax = float(random.choice([4,10,21])/100)
        shipdate = datetime(np.random.randint(2000, 2020), np.random.randint(1, 12), np.random.randint(1, 27))
        returnflag = random.choice(string.ascii_uppercase)
        linestatus = random.choice(string.ascii_uppercase)
        posts.append({"_id":i, "order": order, "quantity": quantity, "extendedprice": extendedprice, 
                    "discount": discount, "tax": tax, "shipdate": shipdate, "returnflag": returnflag, "linestatus": linestatus})

    collection.insert_many(posts)

    collection = db["PartSupp"]
    posts = []
    for i in range(200):
        ps_supplycost = float(np.random.randint(5, 1500) + np.random.randint(0, 99)/100)
        p_key = np.random.randint(1, 20)
        p_mfgr = random.choice(string.ascii_uppercase)*25
        p_type = random.choice(string.ascii_uppercase)*25
        p_size = np.random.randint(1, 50)
        part = {"_id": p_key, "mfgr": p_mfgr, "type": p_type, "size": p_size}

        s_suppkey = np.random.randint(1, 20)
        s_name = random.choice(string.ascii_uppercase)*25
        s_address = random.choice(string.ascii_uppercase)*40
        s_phone = random.choice(string.ascii_uppercase)*15
        s_acctbal = float(np.random.randint(5, 1500) + np.random.randint(0, 99)/100)
        s_comment = random.choice(string.ascii_uppercase)*101
        n_name = random.choice(string.ascii_uppercase)*25
        r_name = random.choice(string.ascii_uppercase)*25
        supplier = {"_id": s_suppkey, "name": s_name, "address": s_address, "phone": s_phone, "acctbal": s_acctbal, "comment": s_comment, "nation_name": n_name, "region_name": r_name}
        PartSupp = {"_id": i, "part": part, "supplier": supplier, "supplycost": ps_supplycost}
        posts.append(PartSupp)
        
    collection.insert_many(posts)

def execute_q1(date):
    results = collection.aggregate( [
    {
        '$match': {
            'shipdate': {
                '$lte': date
            }
        }
    }, {
        '$addFields': {
            'perc_disc': {
                '$subtract': [
                    1, '$discount'
                ]
            }, 
            'perc_tax': {
                '$add': [
                    1, '$tax'
                ]
            }
        }
    }, {
        '$addFields': {
            'disc_price': {
                '$multiply': [
                    '$extendedprice', '$perc_disc'
                ]
            }
        }
    }, {
        '$addFields': {
            'charge': {
                '$multiply': [
                    '$disc_price', '$perc_tax'
                ]
            }
        }
    }, {
        '$group': {
            '_id': {
                'returnflag': '$returnflag', 
                'linestatus': '$linestatus'
            }, 
            'sum_qty': {
                '$sum': '$quantity'
            }, 
            'sum_base_price': {
                '$sum': '$extendedprice'
            }, 
            'sum_disc_price': {
                '$sum': '$disc_price'
            }, 
            'sum_charge': {
                '$sum': '$charge'
            }, 
            'avg_qty': {
                '$avg': '$quantity'
            }, 
            'avg_price': {
                '$avg': '$extendedprice'
            }, 
            'avg_disc': {
                '$avg': '$discount'
            }, 
            'count_order': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            '_id.returnflag': 1, 
            '_id.linestatus': 1
        }
    }
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