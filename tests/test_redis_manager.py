import pytest
from unittest.mock import MagicMock
from Manager.Redis_Manager import RedisManager
import redis
@pytest.fixture
def mock_redis():
    re_manager=RedisManager()
    re_manager.redis_client=MagicMock()
    return re_manager

def test_set_value_success(mock_redis):
    mock_redis.redis_client.set.return_value = True
    result = mock_redis.set_value("test_key", "test_value", ttl_sec=10)
    assert result is True
    mock_redis.redis_client.set.assert_called_once_with(name="test_key", value="test_value", ex=10)
    
def test_set_value_failure(mock_redis):
    mock_redis.redis_client.set.side_effect = redis.RedisError("Redis error")
    result = mock_redis.set_value("test_key", "test_value", ttl_sec=10)
    assert result is False
    
def test_get_value_success(mock_redis):
    mock_redis.redis_client.get.return_value = "test_value"
    result = mock_redis.get_value("test_key")
    assert result == "test_value"
    mock_redis.redis_client.get.assert_called_once_with("test_key")
    
def test_get_value_failure(mock_redis):
    mock_redis.redis_client.get.side_effect = redis.RedisError("Redis error")
    result = mock_redis.get_value("test_key")
    assert result is None
    
def test_delete_key_success(mock_redis):
    mock_redis.redis_client.delete.return_value = 1
    result = mock_redis.delete_key("test_key")
    assert result is True
    mock_redis.redis_client.delete.assert_called_once_with("test_key")

def test_delete_key_failure(mock_redis):
    mock_redis.redis_client.delete.side_effect = redis.RedisError("Redis error")
    result = mock_redis.delete_key("test_key")
    assert result is False  
