from termcolor import colored
import threading
import time
import random
import csv
import os

# Assuming the following classes and global variables are defined in defs.py
from defs import Hops, HopsQueue, Request, Response, DoneList, globalMessageQueue, globalResponseQueue, allQueues, numberOfPartitions, allNodes

condition = threading.Condition()
should_stop = False
transaction_states = {}  # To track the state of each transaction

hop_descriptions = {
    "T1": "Registered student 1",
    "T2": "Viewed submissions of a class",
    "T3_1": "Submitted assignment in Submission table",
    "T3_2": "Updatd submission count in Assignment table",
    "T4_1": "Submitted quiz in Submission table",
    "T4_2": "Update submission count in Quiz table",
    "T5": "Updated student 1 email",
    "T6": "Calculated grade of class from Submission table and updated final grades in Student table",
    "T7_1": "View submission deadlines for Quizzes",
    "T7_2": "View submission deadlines for Assignments"
}

def write_row(operations, filename):
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        for op in operations:
            row = [op["table_name"]] + list(op["data"].values())
            writer.writerow(row)

def update_row(operations, filename):
    temp_filename = filename + ".tmp"

    with open(filename, 'r', newline='') as file, open(temp_filename, 'w', newline='') as tempfile:
        reader = csv.reader(file)
        writer = csv.writer(tempfile)

        for row in reader:
            updated = False
            for op in operations:
                if row[0] == op["table_name"] and row[1] == str(op["primary_key_value"]):
                    new_row = [op["table_name"]] + list(op["data"].values())
                    writer.writerow(new_row)
                    updated = True
                    break
            if not updated:
                writer.writerow(row)

    os.replace(temp_filename, filename)

def read_row(operations, filename):
    results = []
    with open(filename, 'r', newline='') as file:
        reader = list(csv.reader(file))  # Read all lines once and store them in a list
        for op in operations:
            for row in reader:
                if row[0] == op["table_name"] and row[1] == str(op["primary_key_value"]):
                    op["data"].update({key: value for key, value in zip(op["data"].keys(), row[1:])})
                    results.append(op["data"])
                    break
    if not results:
        print(f"No matching row found to read in {filename}")
    return results


def process_queue(queueObj, done_list):
    global transaction_states
    hopsqueue = queueObj.queue

    while not hopsqueue.is_empty():
        hop = hopsqueue.peek()
        if hop.transaction_tag == "T7":
            if not all(done_list.contains(t) for t in ["T3", "T4"]):
                print("Waiting for dependencies to complete for T7...", done_list)
                time.sleep(5)
                continue

        # Process the hop and wait for the response before continuing
        request = Request(hop)
        queueObj.message_queue.put(request)
        with condition:
            condition.notify_all()

        response = queueObj.response_queue.get()
        # time.sleep(5)
        print(colored(f"Queue {queueObj.queueNumber}: Received response from Node {queueObj.queueNumber} " + response.status, 'green'))

        if(hop.transaction_tag in ["T1", "T2", "T5", "T6"]):
            #  print(colored('hello', 'red'), colored('world', 'green'))
            print( colored("Hop for " + hop.transaction_tag + " DONE ======> " + hop_descriptions.get(hop.transaction_tag), 'red'))
        else:
            if(hop.is_last):
                print( colored("Hop for "+ hop.transaction_tag+"_2" + " DONE ======> " + hop_descriptions.get(hop.transaction_tag+"_2"),'red'))
            else:
                print( colored("Hop for " + hop.transaction_tag+"_1" + " DONE ======> " + hop_descriptions.get(hop.transaction_tag+"_1"),'red'))

        if response.status == "abort":
            hopsqueue.remove_all(hop.transaction_tag)
            print(f"Transaction {hop.transaction_tag} aborted. Removed all its hops from the queue.")
            # Reset the state of the transaction
            if hop.transaction_tag in transaction_states:
                del transaction_states[hop.transaction_tag]
        else:
            if hop.operations[0]["operation_type"] == "read":
                print(f"Read value: {response.body}")
            hopsqueue.dequeue()

            if hop.is_last:
                done_list.add(hop.transaction_tag)
                # Reset the state of the transaction
                if hop.transaction_tag in transaction_states:
                    del transaction_states[hop.transaction_tag]
            else:
                # Update the state of the transaction to the next hop
                if hop.transaction_tag in transaction_states:
                    transaction_states[hop.transaction_tag] += 1
                else:
                    transaction_states[hop.transaction_tag] = 1

    print(f"{threading.current_thread().name} done")

def queueThread(queueObj, done_list):
    global transaction_states
    hopsqueue = queueObj.queue

    while not hopsqueue.is_empty():
        hop = hopsqueue.peek()
        transaction_tag = hop.transaction_tag

        if transaction_tag in transaction_states:
            current_hop_index = transaction_states[transaction_tag]
            if current_hop_index != 0:
                # Wait for the previous hop to complete
                with condition:
                    condition.wait()
                continue

        process_queue(queueObj, done_list)

def nodeThread(node):
    message_queue = globalMessageQueue[node.nodeNumber]
    response_queue = globalResponseQueue[node.nodeNumber]
                                         
    while not should_stop:
        if not message_queue.empty():
            request = message_queue.get()
            hop = request.hop
            time.sleep(2)
            if(hop.transaction_tag in ["T1", "T2", "T5", "T6"]):
                print(colored(f"Node {node.nodeNumber}: Received hop " + hop.transaction_tag + f" from queue {node.nodeNumber}", 'blue'))
            else:
                if(hop.is_last):
                    print(colored(f"Node {node.nodeNumber}: Received hop " + hop.transaction_tag+"_2" +  f" from queue {node.nodeNumber}", 'blue'))
                else:
                    print(colored(f"Node {node.nodeNumber}: Received hop " + hop.transaction_tag+"_1" +  f" from queue {node.nodeNumber}", 'blue'))

            status = "commit"
            body = ""

            for operation in hop.operations:
                if operation["operation_type"] == "write":
                    write_row([operation], node.csvFile)
                elif operation["operation_type"] == "update":
                    update_row([operation], node.csvFile)
                elif operation["operation_type"] == "read":
                    read_row([operation], node.csvFile)
                    body += str(operation["data"])  # Collect data from read operations

            response = Response(status, body)
            # time.sleep(2)
            # print(f"Node {node.nodeNumber}: Sending response to Queue {node.nodeNumber}")
            response_queue.put(response)
        else:
            with condition:
                condition.wait()

def create_empty_csv_files(n):
    for i in range(n):
        filename = f"{i}.csv"
        open(filename, 'w').close()

def preload_data():
    # Define the initial data for each CSV file
    initial_data = {
        "0.csv": [
            ["Student", 5, "Bobby Lee", "bobby.lee@example.com"],
            ["Submission", 5, 5, "Assignment", 1],
        ],
        "1.csv": [

            ["Student", 3, "Teddy Swims", "teddy.swims@example.com"],
            ["Student", 4, "Bob Doe", "bob.doe@example.com"],
            ["Student", 2, "Mike Ross", "mike.ross@example.com"],
            ["Submission", 3, 3, "Assignment", 1],
            ["Submission", 4, 4, "Quiz", 1]
        ],
        "2.csv": [
            ["Assignment", 1, 4, "link1", "Friday"],
            ["Assignment", 2, 0, "link2", "Sunday"]
        ],
        "3.csv": [
            ["Quiz", 1, 2, "link2", "Sunday"],
        ]
    }

    # Write the initial data to the corresponding CSV files
    for filename, rows in initial_data.items():
        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file)
            for row in rows:
                writer.writerow(row)

def main():
    global should_stop

    done_list = DoneList()
    hopsqueue1 = allQueues[0].queue
    hopsqueue2 = allQueues[1].queue
    hopsqueue3 = allQueues[2].queue
    hopsqueue4 = allQueues[3].queue

    create_empty_csv_files(numberOfPartitions)

    preload_data()

    # register student
    hop1 = Hops(transaction_tag="T1", is_last=True, operations=[
                {"operation_type": "write", "table_name": "Student", "primary_key_name": "student_id", "primary_key_value": 1, "data": {"student_id": 1, "name": "John Doe", "email": "john.doe@example.com"}}
                ])
    # view submissions of a class
    hop2 = Hops(transaction_tag="T2", is_last=True, operations=[
                {"operation_type": "read", "table_name": "Submission", "primary_key_name": "submission_id", "primary_key_value": 3, "data":{"submission_id": 0, "studnt_id": 0, "submission_type": "", "type_id": 0}}
                ])
    # submit assignment
    hop3 = Hops(transaction_tag="T3", is_last=False, operations=[
                {"operation_type": "write", "table_name": "Submission", "primary_key_name": "submission_id", "primary_key_value": 1, "data": {"submission_id": 1, "studnt_id": 1, "submission_type": "Assignment", "type_id": 1}}
                ])
    # update submission count in Assignment table
    hop4 = Hops(transaction_tag="T3", is_last=True, operations=[
                {"operation_type": "update", "table_name": "Assignment", "primary_key_name": "assignment_id", "primary_key_value": 1, "data": {"assignment_id": 1, "submission_count": 5, "link": "link1", "deadline": "Friday"}}
                ])
    # submit quiz
    hop5 = Hops(transaction_tag="T4", is_last=False, operations=[
                {"operation_type": "write", "table_name": "Submission", "primary_key_name": "submission_id", "primary_key_value": 2, "data": {"submission_id": 2, "student_id": 2, "submission_type": "Quiz", "type_id": 1}}
                ])
    # update submission count in Quiz table
    hop6 = Hops(transaction_tag="T4", is_last=True, operations=[
                {"operation_type": "update", "table_name": "Quiz", "primary_key_name": "quiz_id", "primary_key_value": 1, "data": {"quiz_id": 1, "submission_count": 3, "link": "link2", "deadline":"Sunday"}}
                ])
    # update student email
    hop7 = Hops(transaction_tag="T5", is_last=True,operations=[
                {"operation_type": "update", "table_name": "Student", "primary_key_name": "student_id", "primary_key_value": 1, "data": {"student_id": 1, "name": "John Doe", "email": "johndoe.updated@example.com"}}
                ])
    # calculate grade of a class from Submission table and update student table
    hop8 = Hops(transaction_tag="T6", is_last=True, operations=[
                {"operation_type": "read", "table_name": "Submission", "primary_key_name": "submission_id", "primary_key_value": 3, "data":{"submission_id": 0, "studnt_id": 0, "submission_type": "", "type_id": 0}},
                {"operation_type": "read", "table_name": "Submission", "primary_key_name": "submission_id", "primary_key_value": 4, "data":{"submission_id": 0, "studnt_id": 0, "submission_type": "", "type_id": 0}},
                {"operation_type": "update", "table_name": "Student", "primary_key_name": "student_id", "primary_key_value": 3, "data":{"student_id": 3, "name": "Teddy Swims", "email": "teddy.swims@example.com", "grade": "B+"}},
                {"operation_type": "update", "table_name": "Student", "primary_key_name": "student_id", "primary_key_value": 4, "data":{"student_id": 4, "name": "Bob Doe", "email": "bob.doe@example.com", "grade": "A"}}
                ])
    # view submission deadlines for Quizzes
    hop9 = Hops(transaction_tag="T7", is_last=False, operations=[
                {"operation_type": "read", "table_name": "Quiz", "primary_key_name": "quiz_id", "primary_key_value": 1, "data": {"quiz_id": 0, "submission_count": 0, "link": "", "deadline": ""}}
                ])
    # view submission deadlines for Assignments
    hop10 = Hops(transaction_tag="T7", is_last=True, operations=[
                {"operation_type": "read", "table_name": "Assignment", "primary_key_name": "assignment_id", "primary_key_value": 1, "data": {"assignment_id": 0, "submission_count": 0, "link": "", "deadline": ""}}
                ])
    
    # hopsqueue1 T1, T4, T7
    # hopsqueue2 T2, T4, T7
    # hopsqueue3 T3, T5
    # hopsqueue4 T3, T6
    hopsqueue1.enqueue(hop1)
    hopsqueue2.enqueue(hop2)
    hopsqueue1.enqueue(hop3)
    hopsqueue3.enqueue(hop4)
    hopsqueue2.enqueue(hop5)
    hopsqueue4.enqueue(hop6)
    hopsqueue1.enqueue(hop7)
    hopsqueue2.enqueue(hop8)
    hopsqueue4.enqueue(hop9)
    hopsqueue3.enqueue(hop10)

    # Record start time
    start_time = time.time()

    for i in range(numberOfPartitions):
        t1 = threading.Thread(target=queueThread, args=(allQueues[i], done_list))
        t2 = threading.Thread(target=nodeThread, args=(allNodes[i], ))

        t1.start()
        t2.start()

        allQueues[i].threadID = t1
        allNodes[i].threadID = t2

    for q in allQueues:
        q.threadID.join()

    should_stop = True
    with condition:
        condition.notify_all()
    
    # Record end time
    end_time = time.time()
    
    # Calculate throughput
    total_time = end_time - start_time
    throughput = 10 / total_time if total_time > 0 else 0

    # Print done list and throughput
    print(f"Done List:", done_list)
    print(f"Total time:", total_time)
    print(f"Throughput: {throughput:.2f} hops per second")

    for n in allNodes:
        n.threadID.join()

if __name__ == "__main__":
    main()