import argparse
import json
import pika


def parse_args():
    """ Parse arguments from command line"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--db_file_path', help='Path to database file',
                        required=False, default='C:\\Oren2\\chinook.db')
    parser.add_argument('--server_name', help='Name of rabbit server',
                        required=False, default='localhost')
    parser.add_argument('--queue_name', help='Name of rabbit queue',
                        required=False, default='temp')
    parser.add_argument('--state', help='State name for query',
                        required=False, default='Canada')
    parser.add_argument('--year', help='Year for query',
                        required=False, default=2011)
    parser.add_argument('--genre', help='genre for query',
                        required=False, default='Rock')
    return vars(parser.parse_args())


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


def send_message(chan, queue, db_path, state, year, genre):
    print("Queue = %s, path = %s, state = %s, year = %s, genre = %s" %
          (queue, db_path, state, year, genre))
    send_json = locals()
    channel = send_json.pop('chan')
    queue = send_json.pop('queue')
    send_json['year'] = str(send_json['year'])
    print("### Sending out message: %s, on queue: %s" % (json.dumps(send_json), queue))
    channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(send_json))



def main():
    args = parse_args()
    queue = args['queue_name']
    connection = connect_to_rabbit(args['server_name'])
    chan = create_channel(connection, queue)

    with chan:
        send_message(chan, queue, args['db_file_path'], args['state'],
                     args['year'], args['genre'])

    connection.close()    

if __name__ == "__main__":
    main()