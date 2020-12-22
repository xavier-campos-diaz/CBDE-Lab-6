from neo4j import GraphDatabase, basic_auth
import datetime
import time


# Drop all data
def drop(session):
    session.run("MATCH (n) DETACH DELETE n")


# Print all data
def print_all(session):
    print('The program have inserted the following nodes:')
    for item in session.run('MATCH (n) RETURN n'):
        print(item)


# Create a part node with parameters specified
def create_part(session, identifier, partkey, mfgr, type, size):
    session.run("CREATE (" + identifier + ":Part {partkey:'" + partkey +
                "', mfgr:'" + mfgr + "', type: '" + type + "', size: " + size + "})")


# Create a supplier node with parameters specified
def create_supplier(session, identifier, suppkey, name, accbal, adress, phone, comment, n_name, r_name):
    session.run("CREATE (" + identifier + ":Supplier {suppkey: '" + suppkey +
                "', name: '" + name + "', accbal: " + accbal + ", adress: '" + adress +
                "', phone: '" + phone + "', comment: '" + comment +
                "', n_name: '" + n_name + "', r_name: '" + r_name + "'})")


# Create an order node with parameters specified
def create_order(session, identifier, orderkey, orderdate, shippriority, c_marketsegment, n_name):
    session.run("CREATE (" + identifier + ":Order {orderkey: '" + orderkey + "', orderdate: {date}, shippriority:'" +
                shippriority + "', c_marketsegment: '" + c_marketsegment + "', n_name: '" + n_name + "'})",
                {"date": orderdate})


# Create a line item node with parameters specified
def create_lineitem(session, identifier, orderkey, suppkey, returnflag, quantity,
                    extendedPrice, discount, tax, shipdate, linestatus):
    session.run("CREATE (" + identifier + ":LineItem {orderkey: '" + orderkey +
                "', suppkey: '" + suppkey + "', returnflag: '" + returnflag + "', quantity: " + quantity +
                ", extendedPrice: " + extendedPrice + ", discount: " + discount + ", tax: " + tax +
                ", shipdate: {date2}, linestatus: '" + linestatus + "'})", {"date2": shipdate})


# Create a edge between supplier and part nodes specified in parameters
def create_edge_supplier_part(session, supplier, suppkey, part, partkey, supplycost):
    session.run(
        "MATCH (" + supplier + ":Supplier {suppkey: '" + suppkey + "'}), (" + part + ":Part {partkey: '" + partkey +
        "'}) CREATE (" + supplier + ")-[:ps {supplycost: {suppcost} }]->(" + part + ")",
        {"suppcost": supplycost})


# Create a edge between order and line item nodes specified in parameters
def create_edge_order_lineitem(session, order, orderkey, lineitem):
    session.run("MATCH (" + order + ":Order {orderkey: '" + orderkey + "'}), (" + lineitem +
                ":LineItem {orderkey: '" + orderkey + "'}) CREATE (" + order + ")-[:has]->(" + lineitem + ")")


# Create a edge between line item and supplier nodes specified in parameters
def create_edge_lineitem_supplier(session, lineitem, supplier, suppkey):
    session.run("MATCH (" + lineitem + ":LineItem {suppkey: '" + suppkey + "'}), (" + supplier +
                ":Supplier {suppkey: '" + suppkey + "'}) CREATE (" + lineitem + ")-[:isFrom]->(" + supplier + ")")


# Insert some data in database
def inserts(db):
    print('Starting inserts...')
    session = db.session()

    date = datetime.datetime(2016, 11, 24)
    date2 = datetime.datetime(2016, 11, 25)
    timestamp = time.mktime(date.timetuple())
    timestamp2 = time.mktime(date2.timetuple())

    drop(session)
    create_part(session, 'p1', '1', 'aaaa', 'A', '10')
    create_part(session, 'p2', '2', 'bbbb', 'B', '15')
    create_part(session, 'p3', '3', 'cccc', 'C', '5')
    create_part(session, 'p4', '4', 'dddd', 'D', '2')
    create_supplier(session, 's1', 's1', 'supp1', '1.00', 'Main Street', '111111', 'nothing', 'Spain', 'Barcelona')
    create_supplier(session, 's2', 's2', 'supp2', '2.00', 'Main Street 2', '2222222 ', 'nothing 2', 'Spain',
                    'Barcelona')
    create_order(session, 'o1', 'o1', timestamp, '1', 'MKT1', 'Spain')
    create_order(session, 'o2', 'o2', timestamp, '2', 'MKT1', 'Spain')
    create_lineitem(session, 'l1', 'o1', 's1', 'a', '10', '10.0', '0.1', '2.0', timestamp2, 'a')
    create_lineitem(session, 'l2', 'o1', 's1', 'a', '5', '18.0', '0.5', '3.0', timestamp2, 'a')
    create_lineitem(session, 'l3', 'o1', 's1', 'a', '5', '5.0', '0.05', '1.0', timestamp2, 'a')
    create_lineitem(session, 'l4', 'o2', 's2', 'b', '10', '20.0', '0.3', '3.0', timestamp2, 'b')
    create_lineitem(session, 'l5', 'o2', 's2', 'b', '20', '40.0', '0.5', '1.0', timestamp2, 'b')
    create_lineitem(session, 'l6', 'o2', 's2', 'b', '5', '10.0', '0.2', '2.0', timestamp2, 'b')

    create_edge_supplier_part(session, 's1', 's1', 'p1', '1', 10)
    create_edge_supplier_part(session, 's1', 's1', 'p2', '2', 20)
    create_edge_supplier_part(session, 's1', 's1', 'p3', '3', 30)
    create_edge_supplier_part(session, 's1', 's1', 'p4', '4', 40)
    create_edge_supplier_part(session, 's2', 's2', 'p1', '1', 5)
    create_edge_supplier_part(session, 's2', 's2', 'p2', '2', 10)
    create_edge_supplier_part(session, 's2', 's2', 'p3', '3', 15)
    create_edge_supplier_part(session, 's2', 's2', 'p4', '4', 20)
    create_edge_order_lineitem(session, 'o1', 'o1', 'l1')
    create_edge_order_lineitem(session, 'o2', 'o2', 'l4')
    create_edge_lineitem_supplier(session, 'l1', 's1', 's1')
    create_edge_lineitem_supplier(session, 'l2', 's2', 's2')

    print_all(session)
    print('Finish inserts!\n')

    session.close()
    return db


# Create the neo4j database
def create():
    print('Create and connect with Database')

    db = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
    return inserts(db)


# Create indexes
def indexes(db):
    print('Creating indexes...')
    session = db.session()

    session.run("CREATE INDEX ON :LineItem(shipdate)")
    session.run("CREATE INDEX ON :Order(orderdate)")

    print('Indexes created!')
    session.close()

    print()


# Query 1 code
def query1(db, date):
    print('Query 1 starting...')
    result = \
        db.session().run(" MATCH " +
                         "      ( li:LineItem ) " +
                         " WHERE " +
                         "      li.shipdate <= {date} " +
                         " WITH " +
                         "      li.returnflag                                    AS l_returnflag, " +
                         "      li.linestatus                                    AS l_linestatus, " +
                         "      SUM(li.quantity)                                 AS sum_qty, " +
                         "      SUM(li.extendedPrice)                            AS sum_base_price, " +
                         "      SUM(li.extendedPrice*(1-li.discount))            AS sum_disc_price, " +
                         "      SUM(li.extendedPrice*(1-li.discount)*(1+li.tax)) AS sum_charge, " +
                         "      AVG(li.quantity)                                 AS avg_qty, " +
                         "      AVG(li.extendedPrice)                            AS avg_price, " +
                         "      AVG(li.discount)                                 AS avg_disc, " +
                         "      COUNT(*)                                         AS count_order " +
                         " RETURN " +
                         "      l_returnflag, " +
                         "      l_linestatus, " +
                         "      sum_qty, " +
                         "      sum_base_price, " +
                         "      sum_disc_price, " +
                         "      sum_charge, " +
                         "      avg_qty, " +
                         "      avg_price, " +
                         "      avg_disc, " +
                         "      count_order " +
                         " ORDER BY " +
                         "      l_returnflag       ASC, " +
                         "      l_linestatus       ASC ",
                         {"date": time.mktime(date.timetuple())})

    i = 0
    for item in result:
        i += 1
        print(item)

    if i == 0:
        print("No results for first query")
    print()


# Query 2 code
def query2(db, region, type, size):
    print('Query 2 starting...')
    subquery_result = \
        db.session().run(" MATCH " +
                         "    (su: Supplier)-[res:ps]->() " +
                         " WHERE " +
                         "    su.r_name = {region} " +
                         " RETURN " +
                         "    MIN(res.supplycost) ",
                         {"region": region})

    global min_cost
    for item in subquery_result:
        min_cost = item['MIN(res.supplycost)']

    result = \
        db.session().run(" MATCH " +
                         "     (su: Supplier)-[res:ps]->(p1: Part) " +
                         " WHERE " +
                         "     p1.size = {size}                 AND " +
                         "     p1.type = {type}                 AND " +
                         "     res.supplycost = {suppcost} " +
                         " RETURN " +
                         "     su.accbal   AS s_accbal, " +
                         "     su.name     AS s_name, " +
                         "     su.n_name   AS n_name, " +
                         "     p1.partkey  AS p_partkey, " +
                         "     p1.mfgr     AS p_mfgr, " +
                         "     su.adress   AS s_adress, " +
                         "     su.phone    AS s_phone, " +
                         "     su.comment  AS s_comment " +
                         " ORDER BY " +
                         "     su.accbal   DESC, " +
                         "     su.n_name   ASC, " +
                         "     p1.partkey  ASC ",
                         {"size": size,
                          "type": type,
                          "suppcost": min_cost})
    i = 0
    for item in result:
        i += 1
        print(item)

    if i == 0:
        print("No results for second query")

    print()


# Query 3 code
def query3(db, date1, date2, segment):
    print('Query 3 starting...')
    result = \
        db.session().run(" MATCH " +
                         "     (o1:Order)-[:has]->(l1:LineItem) " +
                         " WHERE " +
                         "     l1.shipdate > {date2}            AND " +
                         "     o1.orderdate < {date1}           AND " +
                         "     o1.c_marketsegment = {segment}   AND " +
                         "     o1.orderkey = l1.orderkey "
                         " WITH " +
                         "      l1.orderkey                                   AS l_orderkey, " +
                         "      o1.orderdate                                  AS o_orderdate, " +
                         "      o1.shippriority                               As o_shippriority, " +
                         "      SUM(l1.extendedPrice*(1-l1.discount))         AS revenue " +
                         " RETURN " +
                         "      l_orderkey, " +
                         "      o_orderdate, " +
                         "      o_shippriority, " +
                         "      revenue " +
                         " ORDER BY " +
                         "      revenue        DESC, " +
                         "      o_orderdate    ASC ",
                         {"date1": time.mktime(date1.timetuple()),
                          "date2": time.mktime(date2.timetuple()),
                          "segment": segment})

    i = 0
    for item in result:
        i += 1
        print(item)

    if i == 0:
        print("No results for third query")
    print()


# Query 4 code
def query4(db, region, date):
    print('Query 4 starting...')

    result = \
        db.session().run(" MATCH " +
                         "      (o:Order)-[:has]->(li:LineItem)-[:isFrom]->(s:Supplier)" +
                         " WHERE " +
                         "      s.r_name = {region}             AND " +
                         "      s.n_name = o.n_name             AND " +
                         "      o.orderdate >= {date}           AND " +
                         "      o.orderdate < {date_year} "
                         " RETURN " +
                         "      o.n_name                                    AS n_name, " +
                         "      SUM(li.extendedPrice*(1-li.discount))       AS revenue " +
                         " ORDER BY " +
                         "      revenue     DESC ",
                         {"date": time.mktime(date.timetuple()),
                          "date_year": time.mktime(date.replace(year=date.year + 1).timetuple()),
                          "region": region})
    i = 0
    for item in result:
        i += 1
        print(item)

    if i == 0:
        print("No results for fourth query")

    print()


# Main function
def run():
    print('Neo4J Laboratory\n')
    db = create()
    indexes(db)
    query1(db, date=datetime.datetime(2016, 11, 28))
    query2(db, region='Barcelona', type='A', size=10)
    query3(db, date1=datetime.datetime(2016, 11, 28), date2=datetime.datetime(2016, 11, 20), segment='MKT1')
    query4(db, region="Barcelona", date=datetime.datetime(2016, 11, 20))
    print('THE END')


if __name__ == '__main__':
    run()