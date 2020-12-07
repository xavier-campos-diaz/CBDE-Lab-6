import pymongo
import numpy as np
from pymongo import MongoClient
import string
import random
import copy
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
    for i in range(600):
        order_key = np.random.randint(1, 20)
        order_date = datetime(np.random.randint(2000, 2020), np.random.randint(1, 12), np.random.randint(1, 27))
        order_shippriority = random.choice(["Urgent", "Non-urgent", "Returning", "Express"])
        customer_mkt_seg = random.choice(["Vip", "New", "Returning", "Single", "Married", "Female", "Male", "Graduated", "Employed", "Unemployed"])
        customer_nation_key = np.random.randint(1, 25)
        order = {"_id": order_key, "order_date": order_date, "shippriority": order_shippriority, "c_mktsegment": customer_mkt_seg, "c_nationkey": customer_nation_key}
        quantity = float(np.random.randint(1, 5))
        extendedprice = float(np.random.randint(5, 1500) + np.random.randint(0, 99)/100)
        discount = float(np.random.randint(0, 65)/100)
        tax = float(random.choice([4,10,21])/100)
        shipdate = datetime(np.random.randint(2000, 2020), np.random.randint(1, 12), np.random.randint(1, 27))
        returnflag = random.choice(["W", "I", "D", "R"])
        linestatus = random.choice(["C", "E", "U", "P"])
        posts.append({"_id":i, "order": order, "quantity": quantity, "extendedprice": extendedprice, 
                    "discount": discount, "tax": tax, "shipdate": shipdate, "returnflag": returnflag, "linestatus": linestatus})

    collection.insert_many(posts)

    collection = db["PartSupp"]
    posts = []
    parts = []
    suppliers = []
    for j in range(30):
        p_key = j
        p_mfgr = random.choice(["Bing Steel", "Crucible Industries", "Doral Steel Inc", "Eco Steel LLC", "Gary Works", "Republic Steel", "U.S. Steel", 
                                "United Steel Corp", "Pittsburgh Steel", "Volkswagen Group", "Daimler", "3 ABC LASURES", "A Bianchini Ingeniero SA",
                                "LORENZO BARROSO SA", "Abasic SL", "Abello Linde SA", "Aceros para la Construcción SA", "Zoetis Inc"])
        p_type = random.choice(["Mechanism", "Microchip", "Structure", "Engine"] )
        p_size = np.random.randint(15, 30)
        part = {"_id": p_key, "mfgr": p_mfgr, "type": p_type, "size": p_size}
        parts.append(part)

    for j in range(20):
        s_suppkey = j
        s_name = ["Analog Inc.", "Amphenol Corp.", "Boyd Corp.", "Cheng Corp.", "Compal Elect", "Dexerials Corp.", "Fujikura Ltd.", "HI-P Ltd.", 
                "VERTEX GMBH", "HARDFORD AB", "Siemens", "Bosch", "Enel", "Hitachi", "IBM", "Panasonic", "PepsiCo", "Renault", "Bayer", "Pfizer"][j]
        s_address = ["Unnamed Road, ", "Industry Avn, ", "Random Plaza, ", "Invented Road, ", "Totally Real Avn, ", "My House Street, ", 
                "Existing Road, ", "Not Fake Plaza, ", "Really Nice Street, ", "Actually a Road, "][j%10] + str(np.random.randint(0, 175))
        s_phone = str(np.random.randint(0, 999999999))
        s_acctbal = float(np.random.randint(5, 1500) + np.random.randint(0, 99)/100)
        s_comment = random.choice(["Reliable", "Unreliable", "Always on time", "Rude employees", "Bad Customer Service", "Average", "Good price-quality relation", "Greedy directionship" ])
        
        n_nationkey = int(np.random.randint(0, 25))
        nations = ["Spain", "Germany", "Sweden", "Russia", "France", "USA", "Canada", "Mexico", "Peru", "Brazil", "China", "Thailand", "Japan", "Vietnam", "India", "Angola", "Morroco", "South Africa", "Kenya", "Uganda", "Australia", "New Zeland", "Tonga", "Fiji", "Samoa"]
        regions = ["Europe", "America", "Asia", "Africa", "Oceania"]
        n_name = nations[n_nationkey]
        r_name = regions[int(n_nationkey/5)]
        supplier = {"_id": s_suppkey, "name": s_name, "address": s_address, "phone": s_phone, "acctbal": s_acctbal, "comment": s_comment, "nation_key": n_nationkey, "nation_name": n_name, "region_name": r_name}
        suppliers.append(supplier)

    counter = 0
    for i in range(len(suppliers)):
        for j in range(len(parts)):
            ps_supplycost = float(np.random.randint(5, 1500) + np.random.randint(0, 99)/100)
            PartSupp = {"_id": counter, "part": parts[j], "supplier": suppliers[i], "supplycost": ps_supplycost}
            posts.append(PartSupp)
            counter += 1
       
    collection.insert_many(posts)

def execute_q1(date):
    collection = db["LineItem"]
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
    print("")
    for x in results:
        print(x)


def execute_q2(p_size, p_type, r_name):
    collection = db["PartSupp"]
    minCostPart = list(collection.aggregate([
    {
        '$match': {
            'supplier.region_name': r_name
        }
    }, {
        '$group': {
            '_id': '$part._id', 
            'minSupplyCost': {
                '$min': '$supplycost'
            }
        }
    }
    ]))

    partsSelection = collection.aggregate([
    {
        '$match': {
            'part.size': p_size, 
            'part.type': p_type, 
            'supplier.region_name': r_name
        }
    }, {
        '$project': {
            '_id': 0, 
            'supplier.acctbal': 1, 
            'supplier.name': 1, 
            'supplier.nation_name': 1, 
            'part._id': 1, 
            'part.mfgr': 1, 
            'supplier.address': 1, 
            'supplier.phone': 1, 
            'supplier.comment': 1, 
            'supplycost': 1
        }
    }, {
        '$sort': {
            'supplier.acctbal': -1, 
            'supplier.nation_name': 1, 
            'supplier.name': 1, 
            'part._id': 1
        }
    }
    ])

    result = []
    for ps in partsSelection:
        part_id = ps['part']['_id']
        supplycost = ps['supplycost']
        for minPart in minCostPart:
            min_id = minPart['_id']
            minSC = minPart['minSupplyCost']
            if part_id == min_id and supplycost == minSC:
                aux = {"_id": part_id, "mfgr": ps['part']['mfgr'], "s_name": ps['supplier']['name'], "s_address": ps['supplier']['address'], 
                        "s_phone": ps['supplier']['phone'], "s_acctbal": ps['supplier']['acctbal'], "s_comment": ps['supplier']['comment']}
                result.append(aux)

    print("")
    for x in result:
        print(x)


def execute_q3(c_mkt, date_1, date_2):
    collection = db["LineItem"]
    result = collection.aggregate([
    {
        '$match': {
            'order.c_mktsegment': c_mkt, 
            'order.order_date': {
                '$lt': date_1
            }, 
            'shipdate': {
                '$gt': date_2
            }
        }
    }, {
        '$addFields': {
            'percDisc': {
                '$subtract': [
                    1, '$discount'
                ]
            }
        }
    }, {
        '$addFields': {
            'discountedPrice': {
                '$multiply': [
                    '$extendedprice', '$percDisc'
                ]
            }
        }
    }, {
        '$group': {
            '_id': {
                'order_key': '$order._id', 
                'order_date': '$order.order_date', 
                'shippriority': '$order.shippriority'
            }, 
            'revenue': {
                '$sum': '$discountedPrice'
            }
        }
    }, {
        '$sort': {
            'revenue': -1, 
            'order.order_date': 1
        }
    }
    ])

    print("")
    for x in result:
        print(x)

def execute_q4(r_name, date):
    collection = db["LineItem"]
    nations_revenue = list(collection.aggregate([
    {
        '$addFields': {
            'percDisc': {
                '$subtract': [
                    1, '$discount'
                ]
            }
        }
    }, {
        '$addFields': {
            'discountedPrice': {
                '$multiply': [
                    '$extendedprice', '$percDisc'
                ]
            }
        }
    }, {
        '$match': {
            'order.order_date': {
                '$gte': date
            }, 
            'order.order_date': {
                '$lt': date.replace( year = date.year + 1)
            }
        }
    }, {
        '$group': {
            '_id': '$order.c_nationkey', 
            'revenue': {
                '$sum': '$discountedPrice'
            }
        }
    }, {
        '$sort': {
            'revenue': -1
        }
    }]))

    collection = db["PartSupp"]
    supplier_nations = list(collection.aggregate([
    {
        '$match': {
            'supplier.region_name': r_name
        }
    }, {
        '$group': {
            '_id': {
                's_nationkey': '$supplier.nation_key', 
                's_nationname': '$supplier.nation_name'
            }
        }
    }
    ]))

    result = []
    for nation in nations_revenue:
        nr_key = nation["_id"]
        for s_nation in supplier_nations:
            sn_key = s_nation["_id"]["s_nationkey"]
            if (nr_key == sn_key):
                aux = {"nation_name": s_nation["_id"]["s_nationname"], "revenue": nation["revenue"]}
                result.append(aux)
    
    print("")
    for x in result:
        print(x)
    
if __name__ == "__main__":
    if ("TPC-H" not in dblists):
        initializeDB() 
    value = printOptions()
    while (value != -1):
        if (value == 1):
            year = int(input("Enter the year: "))
            month = int(input("Enter the month: "))
            day = int(input("Enter the day: "))
            execute_q1(datetime(year, month, day))
        elif (value == 2):
            p_size = int(input("Enter the part size (integer from 15 to 30): "))
            p_type = input("Enter the part type, select one from the following: Mechanism, Microchip, Structure or Engine: ")
            r_name = input("Enter the region name, select one from the following: Europe, America, Asia, Africa or Oceania: ")
            execute_q2(p_size, p_type, r_name)
        elif (value == 3):
            c_mkt = input("Enter the customer market segments, select one from the following: Vip, New, Returning, Single, Married, Female, Male, Graduated, Employed or Unemployed: ")
            year_1 = int(input("Enter the year for date 1: "))
            month_1 = int(input("Enter the month for date 1: "))
            day_1 = int(input("Enter the day for date 1: "))
            year_2 = int(input("Enter the year for date 2: "))
            month_2 = int(input("Enter the month for date 2: "))
            day_2 = int(input("Enter the day for date 2: "))
            execute_q3(c_mkt, datetime(year_1, month_1, day_1), datetime(year_2, month_2, day_2))
        elif (value == 4):
            year = int(input("Enter the year: "))
            month = int(input("Enter the month: "))
            day = int(input("Enter the day: "))
            r_name = input("Enter the region name, select one from the following: Europe, America, Asia, Africa or Oceania: ")
            execute_q4(r_name, datetime(year, month, day))
        print("\n")
        value = printOptions()
    print("Goodbye!")