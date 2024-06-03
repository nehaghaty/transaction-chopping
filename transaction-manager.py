import threading
import time
import random
import csv
import os
from defs import *

condition = threading.Condition()
should_stop = False

# queue thread
def queueThread(queueObj):
    message_queue = queueObj.message_queue
    response_queue = queueObj.response_queue
    hopsqueue = queueObj.queue

    for i in range(len(hopsqueue)):
        message_queue.put(hopsqueue.peek())
        with condition:
            condition.notify_all()

        response = response_queue.get()
        print("Thread 1: Received Response from Thread 2", response)
        hopsqueue.dequeue()

# node thread
def nodeThread(node):
    message_queue = globalMessageQueue[node.nodeNumber]
    response_queue = globalResponseQueue[node.nodeNumber]
                                         
    while not should_stop:
        if not message_queue.empty():
            message = message_queue.get()
            print("Thread 2: Received message from Thread 1:", message)
            if (message.operation_type == "write"):
                write_row(message, node.csvFile)
            elif (message.operation_type == "update"):
                update_row(message, node.csvFile)
            else:
                read_row(message, node.csvFile)

            time.sleep(2)
            response = f"Processed: {message}"
            print("Thread 2: Sending response to Thread 1")
            response_queue.put(response)
        else:
            with condition:
                condition.wait()

def create_empty_csv_files(n):
    for i in range(n):
        filename = f"{i}.csv"
        open(filename, 'w').close()

def write_row(hop, filename):
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        row = [hop.table_name] + list(hop.data.values())
        writer.writerow(row)

def update_row(hop, filename):
    temp_filename = filename + ".tmp"

    with open(filename, 'r', newline='') as file, open(temp_filename, 'w', newline='') as tempfile:
        reader = csv.reader(file)
        writer = csv.writer(tempfile)

        for row in reader:
            if row[0] == hop.table_name and row[1] == str(hop.primary_key_value):
                new_row = [hop.table_name] + list(hop.data.values())
                writer.writerow(new_row)

    os.replace(temp_filename, filename)

def read_row(hop, filename):
    with open(filename, 'r', newline='') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[0] == hop.table_name and row[1] == str(hop.primary_key_value):
                hop.data = {key: value for key, value in zip(hop.data.keys(), row[1:])}
                print(f"Row read from {filename}: {row}")
                return row
    print(f"No matching row found to read in {filename}")
    return None

def main():
    global should_stop

    node1 = Node(0)
    queue1 = Queue(0)
    hopsqueue1 = queue1.queue
    create_empty_csv_files(numberOfPartitions)

    hop1 = Hops(transaction_tag="T1", is_last=False, operation_type="write",
                table_name="Users", primary_key_name="user_id", primary_key_value=1,
                data={"user_id" : 1, "username": "John Doe", "email": "john.doe@example.com"})
    
    hop2 = Hops(transaction_tag="T2", is_last=False, operation_type="update",
            table_name="Users", primary_key_name="user_id", primary_key_value=1,
            data={"user_id": 1, "username": "John Doe", "email": "john.doe@newemail.com"})
    
    hop3 = Hops(transaction_tag="T3", is_last=False, operation_type="read",
                table_name="Users", primary_key_name="user_id", primary_key_value=1,
                data={"user_id": None, "name": None, "email": None})
    
    hopsqueue1.enqueue(hop1)
    hopsqueue1.enqueue(hop2)
    hopsqueue1.enqueue(hop3)
    
    for i in range(numberOfPartitions):
        t1 = threading.Thread(target=queueThread, args=(queue1, ))
        t2 = threading.Thread(target=nodeThread, args=(node1, ))

        t1.start()
        t2.start()

        globalQueuesThreads.append(t1)
        globalNodesThreads.append(t2)

    for t in globalQueuesThreads:
        t.join()

    should_stop = True
    with condition:
        condition.notify_all()

    for t in globalNodesThreads:
        t.join()

if __name__ == "__main__":
    main()








# def send_to_node(hop):
#     # Simulate sending hop to a node and getting a response
#     print(f"Sending hop {hop} to node")
#     time.sleep(random.uniform(0.1, 0.5))  # Simulate network delay
#     # Simulate random success or abort response
#     response = "commit"
#     read_value = hop.data if hop.operation_type == "read" else None
#     return response, read_value

# def process_queue(queue, done_list):
#     while not queue.is_empty():
#         hop = queue.peek()
#         if hop.transaction_tag == "T7":
#             if not all(done_list.contains(t) for t in ["T2", "T3", "T4", "T6"]):
#                 print("Waiting for dependencies to complete for T7...", done_list)
#                 time.sleep(5)
#                 continue

#         response, read_value = send_to_node(hop)
#         if response == "abort":
#             queue.remove_all(hop.transaction_tag)
#             print(f"Transaction {hop.transaction_tag} aborted. Removed all its hops from the queue.")
#         else:
#             if hop.operation_type == "read":
#                 print(f"Read value: {read_value}")
#             queue.dequeue()
#             if hop.is_last:
#                 done_list.add(hop.transaction_tag)
#                 print(f"Transaction {hop.transaction_tag} completed and added to done list.")
#             print(f"Processed hop {hop}")

#     print(f"{threading.current_thread().name} done")

# def main():
#     queue1 = HopsQueue()
#     done_list = DoneList()

#     # Create some hops
#     hop1 = Hops(transaction_tag="T1", is_last=False, operation_type="write",
#                 table_name="Users", primary_key_name="user_id", primary_key_value=1,
#                 data={"name": "John Doe", "email": "john.doe@example.com"})

#     hop2 = Hops(transaction_tag="T1", is_last=True, operation_type="write",
#                 table_name="Workouts", primary_key_name="workout_id", primary_key_value=101,
#                 data={"user_id": 1, "duration": 30, "calories": 300})

#     hop3 = Hops(transaction_tag="T2", is_last=False, operation_type="write",
#                 table_name="DietLogs", primary_key_name="diet_log_id", primary_key_value=201,
#                 data={"user_id": 1, "meal": "Breakfast", "calories": 400})

#     hop4 = Hops(transaction_tag="T2", is_last=True, operation_type="write",
#                 table_name="ProgressReports", primary_key_name="report_id", primary_key_value=301,
#                 data={"user_id": 1, "date": "2023-01-01", "weight": 70.5, "body_fat": 15.0})

#     hop5 = Hops(transaction_tag="T3", is_last=False, operation_type="read",
#                 table_name="Users", primary_key_name="user_id", primary_key_value=1,
#                 data=None)

#     hop6 = Hops(transaction_tag="T3", is_last=True, operation_type="write",
#                 table_name="Users", primary_key_name="user_id", primary_key_value=1,
#                 data={"last_login": "2023-01-02"})

#     hop7 = Hops(transaction_tag="T4", is_last=True, operation_type="read",
#                 table_name="Workouts", primary_key_name="workout_id", primary_key_value=101,
#                 data=None)

#     hop8 = Hops(transaction_tag="T5", is_last=True, operation_type="write",
#                 table_name="ProgressReports", primary_key_name="report_id", primary_key_value=302,
#                 data={"user_id": 1, "date": "2023-01-02", "weight": 70.3, "body_fat": 14.8})

#     hop9 = Hops(transaction_tag="T6", is_last=True, operation_type="write",
#                  table_name="Workouts", primary_key_name="workout_id", primary_key_value=101,
#                  data={"user_id": 1, "duration": 45, "calories": 350})
#     hop10 = Hops(transaction_tag="T7", is_last=True, operation_type="write",
#                 table_name="Users", primary_key_name="user_id", primary_key_value=1,
#                 data={"calorie_count": 1500})

#     queue1.enqueue(hop1)
#     queue1.enqueue(hop2)
#     queue1.enqueue(hop3)
#     queue1.enqueue(hop4)
#     queue1.enqueue(hop5)
#     queue1.enqueue(hop6)
#     queue1.enqueue(hop7)
#     queue1.enqueue(hop8)
#     queue1.enqueue(hop9)
#     queue1.enqueue(hop10)

#     # Record start time
#     start_time = time.time()

#     # Process queue
#     process_queue(queue1, done_list)

#     # Record end time
#     end_time = time.time()

    
#     # Calculate throughput
#     total_time = end_time - start_time
#     throughput = 7 / total_time if total_time > 0 else 0

#     # Print done list and throughput
#     print("Done List:", done_list)
#     print(f"Total time:", total_time)
#     print(f"Throughput: {throughput:.2f} hops per second")

# if __name__ == "__main__":
#     main()