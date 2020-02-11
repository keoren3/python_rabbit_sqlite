import argparse
import csv
import json
import sqlite3
import xml.etree.cElementTree as ET
import pika
from sqlite3 import Error

def parse_args():
    """ Parse arguments from command line"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--server_name', help='Name of rabbit server',
                        required=False, default='localhost')
    parser.add_argument('--queue_name', help='Name of rabbit queue',
                        required=False, default='temp')
    return vars(parser.parse_args())


def connect_to_db(db_path):
    """ Create a database connection to a SQLite database """
    print("db_path = %s" % db_path)
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        print(sqlite3.version)
    except Error as e:
        print("Error %s" % e)
        if conn:
            conn.close()
    return conn


def connect_to_rabbit(server_name):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(server_name))
    except Exception as e:
        print("### Error connecting, exception: %s" % e)
        exit(1)
    return connection


def create_channel(connection, queue_name):
    print("Connecting to rabbit...")
    try:
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)
    except Exception as e:
        print("### ERROR: Failed to connect to rabbit! Exception: %s" % e)
        if connection:
            connection.close()
        exit(1)
    return channel


def create_country_purchases(cur, state):
    """Creates country_purchases.csv file
    -------------------------------------
    Input:
    cur(cursor) - Cursor to the sqlite db.
    state(String) - JSON with name of state
    -------------------------------------
    Returns:
    (String)Sqlite command to get all invoice ID's where the billing country was 'state'
    """
    print("### Creating country_purchases.csv...")
    with open("country_purchases.csv", 'a+', newline='') as f:
        writer = csv.writer(f)
        cur.execute("SELECT COUNT(InvoiceId) FROM invoices WHERE BillingCountry='%s'" %
                    state)
        purchase_data = cur.fetchall()
        writer.writerow([state, purchase_data[0][0]])
    return("SELECT InvoiceId FROM invoices WHERE BillingCountry='%s'" %
           state)


def create_country_total_purchases(cur, cmd, state):
    """Creates country_total_purchases.csv file
    -------------------------------------
    Input:
    cur(cursor) - Cursor to the sqlite db.
    cmd(String) - sqlite command to get all invoice ID's where the billing country was 'state'
    state(String) - JSON with name of state
    -------------------------------------
    Returns:
    (String)Sqlite command to get all track ID's from invoice_items where all the
    invoices ID's where from the billing country 'state'
    """
    print("### Creating country_total_purchases.csv...")
    with open("country_total_purchases.csv", 'a+', newline='') as f:
        writer = csv.writer(f)
        cur.execute("SELECT SUM(Quantity) FROM invoice_items WHERE InvoiceId IN (%s)" %
                    cmd)
        item_data = cur.fetchall()
        writer.writerow([state, item_data[0][0]])
        return("SELECT TrackId FROM invoice_items WHERE InvoiceId IN (%s)" %
               cmd)


def create_country_json(cur, cmd, country_json, state):
    """Creates country_json JSON
    -------------------------------------
    Input:
    cur(cursor) - Cursor to the sqlite db.
    cmd(String) - Sqlite command to get all track ID's from invoice_items where all the
        invoices ID's where from the billing country 'state'
    country_json(Dictionary) - Dictionary to fill in
    state(String) - JSON with name of state
    -------------------------------------
    Returns:
    Nothing (Only fills up the country_json Dictionary with the data
             of what album was baught in the state 'state')
    """
    print("Creating country JSON...")
    cur.execute("SELECT Title from albums WHERE AlbumId IN (%s)" % cmd)
    albums_data = cur.fetchall()
    for album in albums_data:
        country_json[state].append(album[0].replace("'", ""))
    print("Current country_json START")
    print(json.dumps(country_json))
    print("Current country_json END")


def create_xml_file(cur, body_json, country_json):
    """Creates the xml file country_albums.xml
    -------------------------------------
    Input:
    cur(cursor) - Cursor to the sqlite db.
    body_json(Dictionary) - Dictionary with all sent data from rabbit.
    country_json(Dictionary) - Dictionary with all baught albums in a list connected to each state.
    state(String) - JSON with name of state.
    -------------------------------------
    Returns:
    xml_state - The first xml node.
    """
    print("Creating XML file...")
    xml_state = ET.Element(body_json['state'])
    xml_year = ET.SubElement(xml_state, 'y' + body_json['year'])
    xml_genre = ET.SubElement(xml_year, body_json['genre'])
    print("Going over all the albums that were baught in the state '%s' "
          "In the year '%s', in the genre '%s'" %
          (body_json['state'], body_json['year'], body_json['genre']))
    for album in country_json[body_json['state']]:
        cur.execute('SELECT '
                    'SUM(invoice_items.quantity) FROM tracks '
                    'INNER JOIN albums ON albums.albumid = tracks.albumid '
                    'INNER JOIN genres ON genres.genreid = tracks.genreid '
                    'INNER JOIN invoice_items ON invoice_items.trackid = tracks.trackid '
                    'INNER JOIN invoices ON invoices.invoiceid = invoice_items.invoiceid '
                    'WHERE albums.Title="%s" AND genres.name="%s" AND '
                    'invoices.BillingCountry = "%s" AND strftime("%%Y", InvoiceDate) = "%s";' %
                    (album, body_json['genre'], body_json['state'], body_json['year']))
        data = cur.fetchall()
        if data[0][0]:
            ET.SubElement(xml_genre, album).text = str(data[0][0])
        else:
            ET.SubElement(xml_genre, album).text = str(0)

    tree = ET.ElementTree(xml_state)
    tree.write("country_albums.xml")
    return xml_state


def create_tables(cur, country_json, xml_state, body_json):
    """Creates the xml file country_albums.xml
    -------------------------------------
    Input:
    cur(cursor) - Cursor to the sqlite db.
    country_json(Dictionary) - Dictionary with all baught albums in a list connected to each state.
    xml_state(XML Element) - XML Element to the desired state
    body_json(Dictionary) - Dictionary with all sent data from rabbit.
    -------------------------------------
    Returns:
    Nothing (Creates 3 first tables)
    """
    print("Creating the 3 first tables...")
    cur.execute('CREATE TABLE IF NOT EXISTS country_purchases(idx INTEGER PRIMARY KEY, state TEXT, amount INTEGER)')
    with open("country_purchases.csv", 'r') as f:
        for idx, line in enumerate(f.read().split('\n')):
            line_splt = line.split(',')
            try:
                cur.execute('INSERT INTO country_purchases VALUES("%s", "%s", "%s")' % (idx, line_splt[0], line_splt[1]))
            except IndexError:
                pass
    cur.execute('CREATE TABLE IF NOT EXISTS country_total_purchases(idx INTEGER PRIMARY KEY, state TEXT, amount INTEGER)')
    with open("country_total_purchases.csv", 'r') as f:
        for idx, line in enumerate(f.read().split('\n')):
            line_splt = line.split(',')
            try:
                cur.execute('INSERT INTO country_total_purchases VALUES("%s", "%s", "%s")' % (idx, line_splt[0], line_splt[1]))
            except IndexError:
                pass
    cur.execute('CREATE TABLE IF NOT EXISTS country_albums(ID INTEGER PRIMARY KEY, state TEXT, year INTEGER, genre TEXT, album TEXT,  amount INTEGER)')
    for idx, album in enumerate(country_json[body_json['state']]):
        cur.execute('INSERT INTO country_albums VALUES ("%s", "%s", "%s", "%s", "%s", "%s")' %
                    (idx, body_json['state'], body_json['year'], body_json['genre'], album,
                     xml_state[0][0][idx].text))


def create_json_table(cur, country_json, body_json):
    """Creates the json table
    -------------------------------------
    Input:
    cur(cursor) - Cursor to the sqlite db.
    country_json(Dictionary) - Dictionary with all baught albums in a list connected to each state.
    body_json(Dictionary) - Dictionary with all sent data from rabbit.
    -------------------------------------
    Returns:
    Nothing (Creates the JSON table)
    """
    print("Creating table from JSON file")
    cur.execute('CREATE TABLE IF NOT EXISTS state_json(id INTEGER PRIMARY KEY, state TEXT, album TEXT)')
    for idx, album in enumerate(country_json[body_json['state']]):
        cur.execute('INSERT INTO state_json VALUES ("%s", "%s", "%s")' % (idx, body_json['state'], album))


def callback(ch, method, properties, body):
    """Callback function when a message is sent to Rabbit
        Manages all requested functions.
    -------------------------------------
    Input:
    ch - Not used.
    method - Not used.
    properties - Not used.
    body(Dictionary) - Dictionary with all sent data from rabbit.
    -------------------------------------
    Returns:
    Nothing - Manages all functions.
    """
    print("Callback body = '%s'" % body)
    country_json = dict()
    body_json = json.loads(body)
    country_json[body_json['state']] = list()
    print(" [x] Received %s" % body_json)
    with connect_to_db(body_json['db_path']) as db_conn:
        cur = db_conn.cursor()
        cmd = create_country_purchases(cur, body_json['state'])

        cmd = create_country_total_purchases(cur, cmd, body_json['state'])

        create_country_json(cur, cmd, country_json, body_json['state'])

        xml_state = create_xml_file(cur, body_json, country_json)

        create_tables(cur, country_json, xml_state, body_json)

        create_json_table(cur, country_json, body_json)


def main():
    args = parse_args()
    rabbit_con = connect_to_rabbit(args['server_name'])
    channel = create_channel(rabbit_con, args['queue_name'])
    channel.basic_consume(queue=args['queue_name'],
                          auto_ack=True,
                          on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

    rabbit_con.close()

if __name__ == "__main__":
    main()
