import redis
import json
import time
import traceback

class Worker:
    def __init__(self, queue_name="default_queue"):
        self.r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.queue_name = queue_name
        self.processing_queue = f"{queue_name}:processing"
        self.retry_set = "retry_set"

    def run(self):
        print(f"Worker started. Listening on {self.queue_name}...")
        while True:
            job_id = self.r.brpoplpush(self.queue_name, self.processing_queue, timeout=5)
            
            if not job_id:
                continue
                
            print(f"[*] Picking up job: {job_id}")
            self.execute_job(job_id)

    def execute_job(self, job_id):
        job_key = f"job:{job_id}"
        
        # Update State Machine
        self.r.hset(job_key, "status", "running")
        
        try:
            # Get job details
            job_data = self.r.hgetall(job_key)
            
            # Parse args (stored as JSON string)
            import json
            args = json.loads(job_data.get('args', '[]'))
            
            # SIMULATE WORK
            print(f"Executing {job_data['func']} with args {args}...")
            time.sleep(2)
            
            # If successful:
            self.r.hset(job_key, "status", "completed")
            # Remove from processing queue on success
            self.r.lrem(self.processing_queue, 1, job_id)
            print(f"[✓] Job {job_id} completed.")
            
        except Exception as e:
            print(f"[!] Job {job_id} failed: {e}")
            self.handle_failure(job_id, job_key)

    def handle_failure(self, job_id, job_key):
        retries = int(self.r.hget(job_key, "retries_left") or 0)
        max_retries = int(self.r.hget(job_key, "max_retries") or 3)
        
        if retries > 0:
            # Calculate exponential backoff
            attempt = max_retries - retries
            delay = 2 ** attempt  # 2, 4, 8 seconds...
            
            print(f"[!] Scheduling retry #{attempt + 1} in {delay}s")
            
            # Update job state
            self.r.hset(job_key, "retries_left", retries - 1)
            self.r.hset(job_key, "status", "scheduled")
            
            # Add to retry set with timestamp
            retry_time = time.time() + delay
            self.r.zadd(self.retry_set, {job_id: retry_time})
            
            # IMPORTANT: Don't remove from processing queue yet!
            # The job stays in processing until it succeeds or fails permanently
            print(f"[→] Job {job_id} moved to retry set (will retry at {time.ctime(retry_time)})")
            
        else:
            print(f"[✘] Job {job_id} failed permanently")
            self.r.hset(job_key, "status", "failed")
            self.r.lpush("dead_letter_queue", job_id)
            # Remove from processing queue only on permanent failure
            self.r.lrem(self.processing_queue, 1, job_id)