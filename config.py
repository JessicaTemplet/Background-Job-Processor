import redis

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)