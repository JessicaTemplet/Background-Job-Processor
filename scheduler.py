# scheduler.py (improved)
import time
from config import get_redis_client

def run_scheduler():
    r = get_redis_client()
    print("[?] Scheduler active. Watching for retries...")
    
    while True:
        now = time.time()
        # Find jobs whose score (timestamp) is <= now
        ready_jobs = r.zrangebyscore("retry_set", 0, now)
        
        if ready_jobs:
            for job_id in ready_jobs:
                # Check if job is still in processing (hasn't been completed/failed)
                processing_queue = "default_queue:processing"
                job_in_processing = r.lpos(processing_queue, job_id) is not None
                
                if job_in_processing:
                    print(f"[→] Re-queueing job: {job_id}")
                    pipe = r.pipeline()
                    
                    # Remove from processing queue
                    pipe.lrem(processing_queue, 1, job_id)
                    # Add back to main queue
                    pipe.lpush("default_queue", job_id)
                    # Remove from retry set
                    pipe.zrem("retry_set", job_id)
                    # Update status back to queued
                    pipe.hset(f"job:{job_id}", "status", "queued")
                    
                    pipe.execute()
                else:
                    # Job was already completed/failed, just clean up retry set
                    r.zrem("retry_set", job_id)
        
        time.sleep(1)

if __name__ == "__main__":
    run_scheduler()