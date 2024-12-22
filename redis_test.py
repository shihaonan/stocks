import redis
r = redis.Redis(
    host = '47.76.82.6',
    port = 6379,
    password = 'haonan0312'
    
)
r.set("age","20")
b=r.get("age")
print(b)
print(r.get('name'))