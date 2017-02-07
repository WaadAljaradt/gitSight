import redis
#print redis

class RedisConfig(object):
	def __init__(self, host="", port=6379,password='',db=0):
		self.__dict__['r'] = redis.StrictRedis(host=host, port=port,password=password, db=db)

	def __getattr__(self, attr):
		return self.__dict__['r'].get(attr)

	def __setattr__(self, k, v):
		self.__dict__['r'].set(k, v)
