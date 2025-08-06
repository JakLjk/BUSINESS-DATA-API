import sys
from business_data_api.workers.worker import run_worker

queue_name = sys.argv[1] if len(sys.argv) > 1 else 'KRSAPI'

if __name__ == "__main__":
    run_worker(queue_name)
