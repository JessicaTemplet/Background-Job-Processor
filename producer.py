# producer.py
import uuid
import time
import json
from config import get_redis_client

class JobProducer:
    def __init__(self):
        self.r = get_redis_client()

    def enqueue(self, func_name, args=None, retries=3):
        job_id = str(uuid.uuid4())
        job_data = {
            "id": job_id,
            "func": func_name,
            "args": json.dumps(args or []),  # Serialize to JSON
            "retries_left": retries,
            "max_retries": retries,
            "status": "queued",
            "created_at": time.time()
        }
        
        # Store metadata
        self.r.hset(f"job:{job_id}", mapping=job_data)
        # Push to main queue
        self.r.lpush("default_queue", job_id)
        
        print(f"[+] Enqueued: {func_name} ({job_id})")
        return job_id

if __name__ == "__main__":
    p = JobProducer()
    p.enqueue("send_welcome_email", args=["user@example.com"])
    p.enqueue("generate_report", args=[101, "pdf"], retries=5)