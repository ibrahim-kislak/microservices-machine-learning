import redis 
from typing import Any, Optional, List, Union
class RedisManager:
    def __init__(self, host:str ='localhost', port:int=6379, db:int=0, decode_responses:bool=True)-> None:
        self._pool=redis.ConnectionPool(
            host=host,
            port=port,                                
            db=db,                                 
            decode_responses=decode_responses)
        self.redis_client: redis.Redis = redis.Redis(connection_pool=self._pool)
        

    def set_value(self, key:str, value:Any, ttl_sec:Optional[int]=None)->bool:
        
        try:
            return bool(self.redis_client.set(name=key, value=value, ex=ttl_sec))
        except redis.RedisError as e:
            print(f"Error setting value in Redis: {e}")
            return False

    def get_value(self, key:str)->Optional[str]:
        try:
         return self.redis_client.get(key)
        except redis.RedisError as e:
            print(f"Error getting value from Redis: {e}")
            return None  
     
    def delete_key(self, key: str) -> bool:
            try:  
                return self.redis_client.delete(key) > 0
            except redis.RedisError:
                return False

redis_service= RedisManager()