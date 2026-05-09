import random
import os
from queue import Queue
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# ---------------------------------
# User Thread Class
# ---------------------------------
class UserThread:
    def __init__(self, tid, burst_time, arrival_time):
        self.tid = tid
        self.burst_time = burst_time
        self.remaining = burst_time
        self.arrival_time = arrival_time
        self.start_time = None
        self.completion_time = 0

        self.waiting_time = 0
        self.turnaround_time = 0
        self.response_time = 0

        self.status = "Ready"


# ---------------------------------
# Kernel Thread Class
# ---------------------------------
class KernelThread:
    def __init__(self, kid):
        self.kid = kid
        self.current_thread = None

    def is_idle(self):
        return self.current_thread is None


# ---------------------------------
# Scheduler Class
# ---------------------------------
class ManyToManyScheduler:
    def __init__(self, num_kernel_threads, time_quantum, model_name):
        self.kernel_threads = [
            KernelThread(i)
            for i in range(num_kernel_threads)
        ]

        self.ready_queue = Queue()

        self.time = 0
        self.completed = []

        self.cpu_busy_time = 0
        self.time_quantum = time_quantum

        self.mapping_table = {}
        self.gantt_chart = {}
        self.execution_counter = {}
        self.queue_history = []
        self.model_name = model_name
        # Simulated scheduling/context-switch overhead
        self.dispatch_latency = 1
        self.context_switches = 0
        self.thread_state_history = []

    # ---------------------------------
    # Add Threads
    # ---------------------------------
    def add_user_threads(self, threads):
        for t in threads:
            self.ready_queue.put(t)

    # ---------------------------------
    # Dynamic Mapping
    # ---------------------------------
    def schedule(self):

        for kt in self.kernel_threads:

            if kt.is_idle() and not self.ready_queue.empty():

                thread = self.ready_queue.get()

                kt.current_thread = thread

                self.mapping_table[thread.tid] = kt.kid

                thread.status = "Running"

                # First response time
                if thread.start_time is None:

                    # Add realistic scheduler dispatch delay
                    thread.start_time = (
                        self.time
                        + self.dispatch_latency
                    )

                    thread.response_time = max(
                        0,
                        thread.start_time
                        - thread.arrival_time
                    )

    # ---------------------------------
    # Execute Threads
    # ---------------------------------
    def execute(self):

        self.schedule()

        for kt in self.kernel_threads:

            if kt.current_thread:

                thread = kt.current_thread

                # Store Gantt execution with real details
                if kt.kid not in self.gantt_chart:
                    self.gantt_chart[kt.kid] = []

                if thread.tid not in self.execution_counter:
                    self.execution_counter[thread.tid] = 0

                self.execution_counter[thread.tid] += 1

                execute_time = min(
                    self.time_quantum,
                    thread.remaining
                )

                remaining_after = thread.remaining - execute_time

                execution_data = {
                    "thread": f"UT{thread.tid}",
                    "burst_time": thread.burst_time,
                    "remaining_time": max(0, remaining_after),
                    "execution_slot": self.execution_counter[thread.tid],
                    "state": (
                        "Completed"
                        if remaining_after == 0
                        else "Running"
                    ),
                    "kernel": f"KT{kt.kid}",
                    "time": self.time,
                    "quantum_used": execute_time,
                    "arrival_time": thread.arrival_time,
                    "waiting_time": thread.waiting_time,
                    "turnaround_time": thread.turnaround_time,
                    "response_time": thread.response_time
                }

                self.gantt_chart[kt.kid].append(execution_data)

                thread.remaining = remaining_after
                self.cpu_busy_time += execute_time

                # Thread Finished
                if thread.remaining == 0:

                    thread.status = "Finished"

                    thread.completion_time = (
                        self.time + execute_time
                    )

                    thread.turnaround_time = (
                        thread.completion_time
                        - thread.arrival_time
                    )

                    thread.waiting_time = max(
                        0,
                        thread.turnaround_time
                        - thread.burst_time
                    )

                    self.completed.append(thread)

                    print(
                        f"Time {self.time} : "
                        f"UT{thread.tid} completed on KT{kt.kid}"
                    )

                    kt.current_thread = None

                # Context Switch
                else:

                    self.context_switches += 1
                    thread.status = "Ready"

                    self.ready_queue.put(thread)

                    print(
                        f"Time {self.time} : "
                        f"Context Switch UT{thread.tid}"
                    )

                    kt.current_thread = None

        # Store Ready Queue Snapshot
        queue_snapshot = []

        temp_size = self.ready_queue.qsize()

        for _ in range(temp_size):
            temp_thread = self.ready_queue.get()
            queue_snapshot.append(f"UT{temp_thread.tid}")
            self.ready_queue.put(temp_thread)

        self.queue_history.append(queue_snapshot)

        # Update Waiting Time
        size = self.ready_queue.qsize()

        for _ in range(size):

            t = self.ready_queue.get()

            # Only ready threads accumulate waiting
            if t.status == "Ready":
                t.waiting_time += (
                    self.time_quantum
                    + self.dispatch_latency
                )

            self.ready_queue.put(t)

        self.time += self.time_quantum

        state_snapshot = {
            "time": self.time,
            "states": []
        }
        print("\n------ THREAD STATES ------")

        all_threads = []

        # Completed threads
        for thread in self.completed:
            all_threads.append(thread)

        # Ready queue threads
        temp_threads = []

        size = self.ready_queue.qsize()

        for _ in range(size):
            t = self.ready_queue.get()
            temp_threads.append(t)
            self.ready_queue.put(t)

        all_threads.extend(temp_threads)

        # Display states
        for t in all_threads:
            print(f"UT{t.tid} --> {t.status}")

            state_snapshot["states"].append({
                "thread": f"UT{t.tid}",
                "state": t.status,
                "remaining": t.remaining
            })

        self.thread_state_history.append(state_snapshot)

    # ---------------------------------
    # Run Simulation
    # ---------------------------------
    def run(self):

        while (
            not self.ready_queue.empty()
            or any(
                not kt.is_idle()
                for kt in self.kernel_threads
            )
        ):

            self.execute()

        self.print_metrics()

    # ---------------------------------
    # Metrics
    # ---------------------------------
    def print_metrics(self):

        n = len(self.completed)

        avg_wait = round(
            sum(t.waiting_time for t in self.completed)
            / n,
            2
        )

        avg_turnaround = round(
            sum(t.turnaround_time for t in self.completed)
            / n,
            2
        )

        avg_response = round(
            sum(t.response_time for t in self.completed)
            / n,
            2
        )

        if self.time == 0:
            cpu_util = 0
        else:
            cpu_util = round(
                (
                    self.cpu_busy_time
                    / (
                        self.time
                        * len(self.kernel_threads)
                    )
                ) * 100,
                2
            )

        # ---------------------------------
        # Dynamic Context Switch Calculation
        # ---------------------------------

        calculated_switches = 0

        for kt in self.gantt_chart:

            tasks = self.gantt_chart[kt]

            if len(tasks) > 1:
                calculated_switches += len(tasks) - 1

        # ---------------------------------
        # Dynamic Starvation Detection
        # ---------------------------------

        starvation_status = "No Starvation"

        max_wait = max(
            t.waiting_time
            for t in self.completed
        )

        # Heavy queue delay detection
        if (
            avg_wait > 200
            or max_wait > (avg_wait * 3)
            or (
                self.model_name == "Many-to-One"
                and self.ready_queue.qsize() > 30
            )
        ):
            starvation_status = "Possible Starvation"

        # ---------------------------------
        # Dynamic Deadlock Detection
        # ---------------------------------

        deadlock_status = "No Deadlock"

        # Simulated heavy congestion detection
        if (
            self.ready_queue.qsize() > 20
            or avg_wait > 300
            or calculated_switches > 500
        ):
            deadlock_status = "Possible Deadlock"

        self.results = {
            "cpu": max(0, cpu_util),
            "waiting": max(0, avg_wait),
            "turnaround": max(0, avg_turnaround),
            "response": max(0, avg_response),
            "total_time": self.time,
            "context_switches": max(
                calculated_switches,
                len(self.completed)
                - len(self.kernel_threads)
            ),
            "deadlock_status": deadlock_status,
            "starvation_status": starvation_status,
            "thread_states": self.thread_state_history,
            "gantt": []
        }

        for kt in self.gantt_chart:

            self.results["gantt"].append({
                "kernel": f"KT{kt}",
                "tasks": self.gantt_chart[kt]
            })

        print("\n------ FINAL RESULTS ------")

        print(f"Total Time : {self.time}")

        print(f"CPU Utilization : {cpu_util:.2f}%")

        print(f"Average Waiting Time : {avg_wait:.2f}")

        print(f"Average Turnaround Time : {avg_turnaround:.2f}")

        print(f"Average Response Time : {avg_response:.2f}")

        final_switches = max(
            calculated_switches,
            len(self.completed)
            - len(self.kernel_threads)
        )

        print(f"Context Switches : {final_switches}")
        print(f"Deadlock Status : {deadlock_status}")
        print(f"Starvation Status : {starvation_status}")

        print("\n------ PERFORMANCE TABLE ------")

        print(
            f"| {self.model_name} | "
            f"CPU: {cpu_util:.2f}% | "
            f"Waiting: {avg_wait:.2f} | "
            f"Turnaround: {avg_turnaround:.2f} |"
        )

        print("\n------ MAPPING TABLE ------")

        for ut, kt in self.mapping_table.items():
            print(f"UT{ut} --> KT{kt}")

        self.display_gantt_chart()
        self.display_queue_status()
        self.display_performance_graphs()

    # ---------------------------------
    # Display Gantt Chart
    # ---------------------------------
    def display_gantt_chart(self):

        print("\n------ GANTT CHART ------")

        for kt in self.gantt_chart:

            print(f"KT{kt} :", end=" ")

            for task in self.gantt_chart[kt]:

                print(
                    f"| {task['thread']} "
                    f"(Q:{task['quantum_used']}, "
                    f"R:{task['remaining_time']}, "
                    f"State:{task['state']}) ",
                    end=""
                )

            print("|")

    # ---------------------------------
    # Display Queue Visualization
    # ---------------------------------
    def display_queue_status(self):

        print("\n------ READY QUEUE STATUS ------")

        time_slot = 0

        for snapshot in self.queue_history:

            print(f"Time {time_slot} : ", end="")

            if len(snapshot) == 0:
                print("Empty")
            else:
                for thread in snapshot:
                    print(f"| {thread} ", end="")
                print("|")

            time_slot += self.time_quantum

    # ---------------------------------
    # Performance Graphs
    # ---------------------------------
    # ---------------------------------
# Performance Graphs
# ---------------------------------
    def display_performance_graphs(self):

        labels = []
        waiting_times = []
        turnaround_times = []

        for thread in self.completed:
            labels.append(f"UT{thread.tid}")
            waiting_times.append(thread.waiting_time)
            turnaround_times.append(thread.turnaround_time)

        cpu_util = (
            self.cpu_busy_time
            / (self.time * len(self.kernel_threads))
        ) * 100

        idle = 100 - cpu_util

        # Create one window with 3 graphs
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))

        # ---------------------------------
        # Waiting Time Graph
        # ---------------------------------
        axes[0].bar(labels, waiting_times)

        axes[0].set_title(
            f"{self.model_name} - Waiting Time"
        )
        axes[0].set_xlabel("Threads")
        axes[0].set_ylabel("Waiting Time")

        # ---------------------------------
        # Turnaround Time Graph
        # ---------------------------------
        axes[1].bar(labels, turnaround_times)

        axes[1].set_title(
            f"{self.model_name} - Turnaround Time"
        )
        axes[1].set_xlabel("Threads")
        axes[1].set_ylabel("Turnaround Time")

        # ---------------------------------
        # CPU Utilization Pie Chart
        # ---------------------------------
        # Create static directory if missing
        os.makedirs("static", exist_ok=True)

        axes[2].pie(
            [cpu_util, idle],
            labels=["CPU Busy", "CPU Idle"],
            autopct='%1.1f%%'
        )

        axes[2].set_title(
            f"{self.model_name} - CPU Utilization"
        )

        plt.tight_layout()

        # Save graph image instead of opening GUI
        plt.savefig("static/performance_graph.png")
        plt.close()

def simulate(
    choice,
    num_user_threads_input,
    time_quantum_input,
    shared_bursts=None,
    shared_arrivals=None
):

    num_user_threads = num_user_threads_input

    # ---------------------------------
    # SAME WORKLOAD FOR EVERY MODEL
    # ---------------------------------

    default_bursts = [
        7, 5, 9, 4, 8,
        6, 10, 5, 7, 8,
        6, 9, 5, 4, 8
    ]

    bursts = []
    arrivals = []

    for i in range(num_user_threads):

        if shared_bursts:
            burst = shared_bursts[i]
        else:
            burst = default_bursts[
                i % len(default_bursts)
            ]

        if shared_arrivals:
            arrival = shared_arrivals[i]
        else:
            arrival = i

        bursts.append(burst)
        arrivals.append(arrival)

    if choice == 1:
        num_kernel_threads = 1
        model_name = "Many-to-One"

    elif choice == 2:
        num_kernel_threads = num_user_threads
        model_name = "One-to-One"

    else:
        num_kernel_threads = 3
        model_name = "Many-to-Many"

    time_quantum = time_quantum_input

    threads = []

    for i in range(num_user_threads):

        thread = UserThread(
            i,
            bursts[i],
            arrivals[i]
        )

        threads.append(thread)

    print("\n==============================")
    print(f"Running Model : {model_name}")
    print("==============================")

    scheduler = ManyToManyScheduler(
        num_kernel_threads,
        time_quantum,
        model_name
    )

    scheduler.add_user_threads(threads)

    scheduler.run()

    return scheduler.results


# ---------------------------------
# Flask Backend
# ---------------------------------
app = Flask(__name__)
CORS(app)


# ---------------------------------
# Serve HTML Frontend
# ---------------------------------
@app.route("/")
def home():
    return send_from_directory(
        ".",
        "simulator.html"
    )


# ---------------------------------
# Shared Workload Cache
# ---------------------------------
shared_workload = {
    "bursts": [],
    "arrivals": []
}

# ---------------------------------
# Simulation API
# ---------------------------------
@app.route("/simulate", methods=["POST"])
def simulate_api():

    global shared_workload

    data = request.get_json()

    model = data["model"]
    threads = int(data["threads"])
    quantum = int(data["quantum"])

    # ---------------------------------
    # CREATE ONE FIXED WORKLOAD
    # ---------------------------------

    if (
        len(shared_workload["bursts"]) != threads
    ):

        default_bursts = [
            7, 5, 9, 4, 8,
            6, 10, 5, 7, 8,
            6, 9, 5, 4, 8
        ]

        shared_workload["bursts"] = [
            default_bursts[
                i % len(default_bursts)
            ]
            for i in range(threads)
        ]

        shared_workload["arrivals"] = [
            i for i in range(threads)
        ]

    if model == "Many-to-One":
        choice = 1

    elif model == "One-to-One":
        choice = 2

    else:
        choice = 3

    response_data = simulate(
        choice,
        threads,
        quantum,
        shared_workload["bursts"],
        shared_workload["arrivals"]
    )

    print("Frontend Request Received")
    print(response_data)

    return jsonify(response_data)

# ---------------------------------
# Optional CLI Runner
# ---------------------------------
def run_simulation_backend(
    choice,
    num_threads,
    quantum
):
    simulate(
        choice,
        num_threads,
        quantum,
        shared_workload.get("bursts"),
        shared_workload.get("arrivals")
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)