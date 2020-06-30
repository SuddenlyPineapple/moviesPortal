import redis


class RedisClient:
    def __init__(self, host, port, db):
        self._redis = redis.Redis(host=host, port=port, db=db)

    def set(self, key, value):
        return self._redis.set(key, value)

    def get(self, key):
        return self._redis.get(key)

    def expire(self, key, time):
        return self._redis.expire(key, time)

    def clearDB(self):
        return self._redis.flushdb()

    def rpush(self, queueName, value):
        return self._redis.rpush(queueName, value)

    def lrange(self, queueName, _from, _to):
        return self._redis.lrange(queueName, _from, _to)

    def ltrim(self, queueName, _from, _to):
        return self._redis.ltrim(queueName, _from, _to)

    def clearQueue(self, queueName):
        currentQueueData = self._redis.lrange(queueName, 0, -1)
        self._redis.ltrim(queueName, len(currentQueueData), -1)
