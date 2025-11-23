import sys
import socket
import time
import random
import threading

# ================= CONFIGURATION =================
HOST = "127.0.0.1"
PORT = 6969
CACHE_SIZE = 100
TEST_DURATION = 30 # Set to 60 for final report
CLIENT_COUNTS = [1, 2, 4, 8, 16, 32, 40, 45, 50]

# Valid Options
WORKLOADS = ["GET_POPULAR", "GET_ALL", "PUT_ALL", "GET_PUT_MIX"]
CONN_MODES = ["KEEP_ALIVE", "CLOSE"]

# ================= HELPER FUNCTIONS =================

def generate_payload(mode, conn_header):
    # We inject the connection header (keep-alive or close) dynamically
    header = f"HTTP/1.1\r\nConnection: {conn_header}\r\n\r\n"

    if mode == "GET_POPULAR":
        key = random.randint(1, 50)
        return f"GET /get?key={key} {header}"

    elif mode == "GET_ALL":
        key = random.randint(1, 10000000)
        return f"GET /get?key={key} {header}"

    elif mode == "PUT_ALL":
        key = random.randint(1, 10000000)
        heavy_val = "X" * 512 # Heavy payload
        return f"GET /set?key={key}&value={heavy_val} {header}"
            
    elif mode == "GET_PUT_MIX":
        key = random.randint(1, 10000000)
        val = f"mix_{random.randint(1,1024)}"
        rand_val = random.random()
        if rand_val < 0.5:   return f"GET /get?key={key} {header}"
        elif rand_val < 0.9: return f"GET /set?key={key}&value={val} {header}"
        else:                return f"GET /delete?key={key} {header}"
         
    return None

# --- WORKER 1: PERSISTENT (High Throughput) ---
def worker_persistent(thread_id, duration, mode, results, index):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.connect((HOST, PORT))
        sock.settimeout(2.0)
    except:
        return 

    start_time = time.time()
    local_reqs = 0
    local_latency_sum = 0.0

    while (time.time() - start_time) < duration:
        payload = generate_payload(mode, "keep-alive")
        try:
            t0 = time.time()            
            sock.sendall(payload.encode())
            resp = sock.recv(4096) # Bigger buffer for safety           
            t1 = time.time()
            
            if len(resp) == 0: break
            local_reqs += 1
            local_latency_sum += (t1 - t0)
        except:
            break 

    sock.close()
    results[index] = (local_reqs, local_latency_sum)

# --- WORKER 2: SHORT-LIVED (High Overhead) ---
def worker_short_lived(thread_id, duration, mode, results, index):
    start_time = time.time()
    local_reqs = 0
    local_latency_sum = 0.0

    while (time.time() - start_time) < duration:
        try:
            # 1. Connect
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((HOST, PORT))
            sock.settimeout(2.0)

            payload = generate_payload(mode, "close")
            
            # 2. Send/Recv
            t0 = time.time()            
            sock.sendall(payload.encode())
            resp = sock.recv(4096)            
            t1 = time.time()
            
            if len(resp) > 0:
                local_reqs += 1
                local_latency_sum += (t1 - t0)
            
            # 3. Close Immediately
            sock.close()
        except:
            # Connection Refused / Timeout (Expected at high load)
            continue

    results[index] = (local_reqs, local_latency_sum)

# --- MANAGER ---
def run_concurrency_level(mode, conn_mode, num_clients):
    print(f"   Running {num_clients} clients...", end=" ", flush=True)

    threads = []
    results = [(0, 0.0)] * num_clients
    
    # Select the correct worker function
    target_func = worker_persistent if conn_mode == "KEEP_ALIVE" else worker_short_lived

    start_time = time.time()

    for i in range(num_clients):
        t = threading.Thread(target=target_func, args=(i, TEST_DURATION, mode, results, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    
    actual_duration = time.time() - start_time

    total_requests = sum(r[0] for r in results)
    total_latency = sum(r[1] for r in results)

    if total_requests == 0:
        print("-> 0 Reqs | 0.00 RPS | 0.00 ms")
        return 0.0, 0.0

    avg_throughput = total_requests / actual_duration
    avg_latency_sec = total_latency / total_requests
    avg_latency_ms = avg_latency_sec * 1000

    print(f"-> {total_requests:<10} | {avg_throughput:<10.2f} RPS | {avg_latency_ms:.2f} ms")
    return avg_throughput, avg_latency_ms

# ================= MAIN EXECUTION =================

if __name__ == "__main__":
    
    if len(sys.argv) != 3:
        print("\nERROR: Usage: python3 load_gen.py <WORKLOAD> <CONNECTION>")
        print("Workloads:   GET_POPULAR, GET_ALL, PUT_ALL")
        print("Connection:  KEEP_ALIVE, CLOSE")
        sys.exit(1)
    
    target_mode = sys.argv[1].upper()
    conn_mode = sys.argv[2].upper()

    if target_mode not in WORKLOADS or conn_mode not in CONN_MODES:
        print(f"ERROR: Invalid Arguments.")
        sys.exit(1)

    print(f"\n--- STARTING BENCHMARK ---")
    print(f"Workload:   {target_mode}")
    print(f"Connection: {conn_mode}")
    
    # Only warmup for Persistent + CPU Bound test
    if target_mode == "GET_POPULAR" :
        print("   [Warmup] Pre-loading keys...")
        try:
            s = socket.socket(); s.connect((HOST, PORT))
            for i in range(1, 51): s.sendall(f"GET /set?key={i}&value=warm HTTP/1.1\r\n\r\n".encode()); s.recv(1024)
            s.close(); print("Done.")
        except: pass

    print("="*80)
    print(f"{'CLIENTS':<10} | {'REQUESTS':<10} | {'THROUGHPUT':<14} | {'AVG LATENCY (ms)':<15}")
    print("-" * 80)

    for n_clients in CLIENT_COUNTS:
        run_concurrency_level(target_mode, conn_mode, n_clients)
        time.sleep(2)
        
    print("\nBenchmark Complete.")