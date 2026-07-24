import pytest
from unittest.mock import MagicMock , patch
from Manager.rabbitmq_manager import RabbitMQManager
import pika
@pytest.fixture
def mock_rabbitmq():
    
    with patch('Manager.rabbitmq_manager.pika.BlockingConnection') as mock_connection:
        manager=RabbitMQManager()
        yield manager, mock_connection
        if hasattr(manager, 'connection') and manager.connection is not None:
            manager.close_connection()

def test_connection_success(mock_rabbitmq):
    manager , mock_conn= mock_rabbitmq
    mock_connetion_instance = MagicMock()
    mock_channel_instance=MagicMock()
    
    mock_conn.return_value = mock_connetion_instance
    mock_connetion_instance.channel.return_value = mock_channel_instance
    
    manager.connect()
    mock_conn.assert_called_once()
    assert manager.connection == mock_connetion_instance
    assert manager.channel == mock_channel_instance 

def test_connection_failure(mock_rabbitmq):
    manager , mock_conn= mock_rabbitmq
    mock_conn.side_effect =pika.exceptions.AMQPConnectionError("Connection failed")
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        manager.connect()
    assert manager.connection is None
    assert manager.channel is None
    
def test_publish_message_success(mock_rabbitmq):
    manager, mock_conn = mock_rabbitmq
    
    # Arrange
    mock_connection_instance = MagicMock()
    mock_channel_instance = MagicMock()
    
    mock_conn.return_value = mock_connection_instance
    mock_connection_instance.channel.return_value = mock_channel_instance
    
    manager.connect()
    
    # Act
    test_msg = "test_data"
    test_key = "my_queue"
    test_ex = "my_exchange"
    
    manager.publish_message(message=test_msg, routing_key=test_key, exchange=test_ex)
    
    mock_channel_instance.basic_publish.assert_called_once_with(
        exchange=test_ex,
        routing_key=test_key,
        body=test_msg,
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
    )

def test_publish_message_failure(mock_rabbitmq):
    manager, mock_conn = mock_rabbitmq
    
    mock_connection_instance = MagicMock()
    mock_channel_instance = MagicMock()
    
    mock_conn.return_value = mock_connection_instance
    mock_connection_instance.channel.return_value = mock_channel_instance
    
    manager.connect()
    
    mock_channel_instance.basic_publish.side_effect = pika.exceptions.AMQPChannelError(404, "Channel error")
    
    with pytest.raises(pika.exceptions.AMQPChannelError):
        manager.publish_message(message="fail_msg", routing_key="test_key")    
        
def test_get_channel_creates_new_connection_when_none(mock_rabbitmq):
    manager, mock_conn = mock_rabbitmq
    
    mock_connection_instance = MagicMock()
    mock_channel_instance = MagicMock()
    
    mock_conn.return_value = mock_connection_instance
    mock_connection_instance.channel.return_value = mock_channel_instance

    manager.channel = None

    # Act
    channel = manager.get_channel()

    # Assert
    mock_conn.assert_called_once()
    assert channel == mock_channel_instance


def test_get_channel_returns_existing_channel_if_open(mock_rabbitmq):
    manager, mock_conn = mock_rabbitmq
    
    mock_existing_channel = MagicMock()
    mock_existing_channel.is_closed = False
    
    manager.channel = mock_existing_channel

    # Act
    channel = manager.get_channel()

    # Assert
    mock_conn.assert_not_called()
    assert channel == mock_existing_channel


def test_get_channel_raises_error_if_connection_fails(mock_rabbitmq):
    manager, mock_conn = mock_rabbitmq
    
    mock_conn.side_effect = pika.exceptions.AMQPConnectionError("Connection failed")
    manager.channel = None

    with pytest.raises(pika.exceptions.AMQPConnectionError):
        manager.get_channel()
        
def test_close_connection_success(mock_rabbitmq):
    manager, _ = mock_rabbitmq
    
    mock_connection = MagicMock()
    mock_connection.is_open = True
    manager.connection = mock_connection

    # Act
    manager.close_connection()

    mock_connection.close.assert_called_once()


def test_close_connection_when_already_closed_or_none(mock_rabbitmq):
    manager, _ = mock_rabbitmq
    
    manager.connection = None
    manager.close_connection()  
    
    mock_connection = MagicMock()
    mock_connection.is_open = False
    manager.connection = mock_connection
    
    manager.close_connection()

    mock_connection.close.assert_not_called()


def test_close_connection_failure_raises_exception(mock_rabbitmq):
    manager, _ = mock_rabbitmq
    
    mock_connection = MagicMock()
    mock_connection.is_open = True
    mock_connection.close.side_effect = pika.exceptions.AMQPConnectionError("Close error")
    manager.connection = mock_connection

    # Act & Assert
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        manager.close_connection()
        
    manager.connection = None
