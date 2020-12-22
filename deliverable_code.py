import numpy as np
import string
import random
from datetime import datetime
from neo4j import GraphDatabase, basic_auth
db = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
session = db.session()
    
def printOptions():
    print("Enter 1 to execute query 1")
    print("Enter 2 to execute query 2")
    print("Enter 3 to execute query 3")
    print("Enter 4 to execute query 4")
    print("Enter -1 to exit this program")
    return int(input("What do you want to do? "))

def createPart(identifier, partkey, mfgr, type, size):
    session.run("CREATE (" + identifier + ":Part {partkey:'" + partkey +
                "', mfgr:'" + mfgr + "', type: '" + type + "', size: " + size + "})")

def createSupplier(identifier, suppkey, name, address, phone, acctbal, comment, n_name, r_name):
    session.run("CREATE (" + identifier + ":Supplier {suppkey: '" + suppkey +
                "', name: '" + name + "', address: '" + address +
                "', phone: '" + phone + "', acctbal: '" + acctbal + "', comment: '" + comment +
                "', n_name: '" + n_name + "', r_name: '" + r_name + "'})")

def create_order(identifier, orderkey, orderdate, shippriority, c_marketsegment, n_name):
    session.run("CREATE (" + identifier + ":Order {orderkey: '" + orderkey + "', orderdate: '" + orderdate + "', shippriority:'" +
                shippriority + "', c_marketsegment: '" + c_marketsegment + "', n_name: '" + n_name + "'})")

def createLineitem(identifier, orderkey, suppkey, quantity, extendedPrice, 
                    discount, tax, returnflag, linestatus, shipdate):
    session.run("CREATE (" + identifier + ":LineItem {orderkey: '" + orderkey +
                "', suppkey: '" + suppkey + "', quantity: '" + quantity + "', extendedPrice: '" + extendedPrice + "', discount: '" + discount + 
                "', tax: '" + tax  +  "', returnflag: '" + returnflag + "', linestatus: '" + linestatus + "', shipdate: '" + shipdate + "'})")

def initializeDB():
    print("Initializing DB!")

    for j in range(30):
        p_ident = "p" + str(j)
        p_mfgr = random.choice(["Bing Steel", "Crucible Industries", "Doral Steel Inc", "Eco Steel LLC", "Gary Works", "Republic Steel", "U.S. Steel", 
                                "United Steel Corp", "Pittsburgh Steel", "Volkswagen Group", "Daimler", "3 ABC LASURES", "A Bianchini Ingeniero SA",
                                "LORENZO BARROSO SA", "Abasic SL", "Abello Linde SA", "Aceros para la Construcción SA", "Zoetis Inc"])
        p_type = random.choice(["Mechanism", "Microchip", "Structure", "Engine"] )
        p_size = np.random.randint(15, 30)
        createPart(p_ident, str(j), p_mfgr, p_type, str(p_size))

    for j in range(20):
        s_ident = "s" + str(j)
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
        createSupplier(s_ident, str(j), s_name, s_address, str(s_phone), str(s_acctbal), s_comment, n_name, r_name)
    
    for i in range(20):
        order_ident = "o" + str(i)
        order_date = datetime(np.random.randint(2000, 2020), np.random.randint(1, 12), np.random.randint(1, 27))
        order_shippriority = random.choice(["Urgent", "Non-urgent", "Returning", "Express"])
        customer_mkt_seg = random.choice(["Vip", "New", "Returning", "Single", "Married", "Female", "Male", "Graduated", "Employed", "Unemployed"])
        n_nationkey = int(np.random.randint(0, 25))
        nations = ["Spain", "Germany", "Sweden", "Russia", "France", "USA", "Canada", "Mexico", "Peru", "Brazil", "China", "Thailand", "Japan", "Vietnam", "India", "Angola", "Morroco", "South Africa", "Kenya", "Uganda", "Australia", "New Zeland", "Tonga", "Fiji", "Samoa"]
        n_name = nations[n_nationkey]
        create_order(order_ident, str(i), str(order_date), order_shippriority, customer_mkt_seg, n_name)

    for i in range(200):
        order_key = np.random.randint(1, 20)
        supp_key = np.random.randint(1, 20)
        l_ident = "l" + str(i)
        quantity = float(np.random.randint(1, 5))
        extendedprice = float(np.random.randint(5, 1500) + np.random.randint(0, 99)/100)
        discount = float(np.random.randint(0, 65)/100)
        tax = float(random.choice([4,10,21])/100)
        shipdate = datetime(np.random.randint(2000, 2020), np.random.randint(1, 12), np.random.randint(1, 27))
        returnflag = random.choice(["W", "I", "D", "R"])
        linestatus = random.choice(["C", "E", "U", "P"])
        createLineitem(l_ident, str(order_key), str(supp_key), str(quantity), str(extendedprice), str(discount), str(tax), str(returnflag), str(linestatus), str(shipdate))


def execute_q1(date):
    print("Not implemented")


def execute_q2(p_size, p_type, r_name):
    print("Not implemented")

def execute_q3(c_mkt, date_1, date_2):
    print("Not implemented")

def execute_q4(r_name, date):
    print("Not implemented")
    
def isDbEmpty():
    for item in session.run('MATCH (n) RETURN 1 LIMIT 1'):
        return False
    return True


if __name__ == "__main__":
    if (isDbEmpty()):
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