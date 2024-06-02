from collections import deque
import threading
import time
import random

class Hops:
    def __init__(self, transaction_tag, is_last, operation_type, table_name, primary_key_name, primary_key_value, data):
        self.transaction_tag = transaction_tag
        self.is_last = is_last
        self.operation_type = operation_type  # 'read' or 'write'
        self.table_name = table_name
        self.primary_key_name = primary_key_name
        self.primary_key_value = primary_key_value
        self.data = data

    def __repr__(self):
        return (f"Hops(transaction_tag={self.transaction_tag}, is_last={self.is_last}, "
                f"operation_type={self.operation_type}, table_name={self.table_name}, "
                f"primary_key_name={self.primary_key_name}, primary_key_value={self.primary_key_value}, "
                f"data={self.data})")

class HopsQueue:
    def __init__(self):
        self.queue = deque()

    def enqueue(self, hop):
        self.queue.append(hop)

    def dequeue(self):
        if self.is_empty():
            raise IndexError("dequeue from an empty queue")
        return self.queue.popleft()

    def is_empty(self):
        return len(self.queue) == 0

    def peek(self):
        if self.is_empty():
            raise IndexError("peek from an empty queue")
        return self.queue[0]

    def __len__(self):
        return len(self.queue)

    def __repr__(self):
        return f"HopsQueue({list(self.queue)})"

    def remove_all(self, transaction_tag):
        self.queue = deque([hop for hop in self.queue if hop.transaction_tag != transaction_tag])

class DoneList:
    def __init__(self):
        self.done_list = []

    def add(self, transaction_tag):
        if transaction_tag not in self.done_list:
            self.done_list.append(transaction_tag)

    def contains(self, transaction_tag):
        return transaction_tag in self.done_list

    def __repr__(self):
        return f"DoneList({self.done_list})"

def send_to_node(hop):
    # Simulate sending hop to a node and getting a response
    print(f"Sending hop {hop} to node")
    time.sleep(random.uniform(0.1, 0.5))  # Simulate network delay
    # Simulate random success or abort response
    response = "commit"
    read_value = hop.data if hop.operation_type == "read" else None
    return response, read_value

def process_queue(queue, done_list):
    while not queue.is_empty():
        hop = queue.peek()
        if hop.transaction_tag == "T7":
            if not all(done_list.contains(t) for t in ["T2", "T3", "T4", "T6"]):
                print("Waiting for dependencies to complete for T7...", done_list)
                time.sleep(5)
                continue

        response, read_value = send_to_node(hop)
        if response == "abort":
            queue.remove_all(hop.transaction_tag)
            print(f"Transaction {hop.transaction_tag} aborted. Removed all its hops from the queue.")
        else:
            if hop.operation_type == "read":
                print(f"Read value: {read_value}")
            queue.dequeue()
            if hop.is_last:
                done_list.add(hop.transaction_tag)
                print(f"Transaction {hop.transaction_tag} completed and added to done list.")
            print(f"Processed hop {hop}")

    print(f"{threading.current_thread().name} done")

def main():
    queue1 = HopsQueue()
    done_list = DoneList()

    # Create some hops
    hop1 = Hops(transaction_tag="T1", is_last=False, operation_type="write",
                table_name="Users", primary_key_name="user_id", primary_key_value=1,
                data={"name": "John Doe", "email": "john.doe@example.com"})

    hop2 = Hops(transaction_tag="T1", is_last=True, operation_type="write",
                table_name="Workouts", primary_key_name="workout_id", primary_key_value=101,
                data={"user_id": 1, "duration": 30, "calories": 300})

    hop3 = Hops(transaction_tag="T2", is_last=False, operation_type="write",
                table_name="DietLogs", primary_key_name="diet_log_id", primary_key_value=201,
                data={"user_id": 1, "meal": "Breakfast", "calories": 400})

    hop4 = Hops(transaction_tag="T2", is_last=True, operation_type="write",
                table_name="ProgressReports", primary_key_name="report_id", primary_key_value=301,
                data={"user_id": 1, "date": "2023-01-01", "weight": 70.5, "body_fat": 15.0})

    hop5 = Hops(transaction_tag="T3", is_last=False, operation_type="read",
                table_name="Users", primary_key_name="user_id", primary_key_value=1,
                data=None)

    hop6 = Hops(transaction_tag="T3", is_last=True, operation_type="write",
                table_name="Users", primary_key_name="user_id", primary_key_value=1,
                data={"last_login": "2023-01-02"})

    hop7 = Hops(transaction_tag="T4", is_last=True, operation_type="read",
                table_name="Workouts", primary_key_name="workout_id", primary_key_value=101,
                data=None)

    hop8 = Hops(transaction_tag="T5", is_last=True, operation_type="write",
                table_name="ProgressReports", primary_key_name="report_id", primary_key_value=302,
                data={"user_id": 1, "date": "2023-01-02", "weight": 70.3, "body_fat": 14.8})

    hop9 = Hops(transaction_tag="T6", is_last=True, operation_type="write",
                 table_name="Workouts", primary_key_name="workout_id", primary_key_value=101,
                 data={"user_id": 1, "duration": 45, "calories": 350})
    hop10 = Hops(transaction_tag="T7", is_last=True, operation_type="write",
                table_name="Users", primary_key_name="user_id", primary_key_value=1,
                data={"calorie_count": 1500})

    queue1.enqueue(hop1)
    queue1.enqueue(hop2)
    queue1.enqueue(hop3)
    queue1.enqueue(hop4)
    queue1.enqueue(hop5)
    queue1.enqueue(hop6)
    queue1.enqueue(hop7)
    queue1.enqueue(hop8)
    queue1.enqueue(hop9)
    queue1.enqueue(hop10)

    # Record start time
    start_time = time.time()

    # Process queue
    process_queue(queue1, done_list)

    # Record end time
    end_time = time.time()

    
    # Calculate throughput
    total_time = end_time - start_time
    throughput = 7 / total_time if total_time > 0 else 0

    # Print done list and throughput
    print("Done List:", done_list)
    print(f"Total time:", total_time)
    print(f"Throughput: {throughput:.2f} hops per second")

if __name__ == "__main__":
    main()