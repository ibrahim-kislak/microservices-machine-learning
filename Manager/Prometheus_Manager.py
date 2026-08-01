import os
import time
from typing import Optional,Generator
from contextlib import contextmanager
from prometheus_client import start_http_server,histogram,Counter,Gauge

class PrometheusManager:
    def __init__(self,host : Optional[str] = None , port : Optional[int] = None)->None:
        self.host:str = host or os.getenv('PROMETHEUS_HOST','0.0.0.0'),
        self.port:int = port or int(os.getenv('PROMETHEUS_PORT',8000))
        self._is_server_started:bool = False
       
        self.prediction_request_counter:Counter = Counter('prediction_requests_total','Total number of prediction requests',labelnames=["status"])
        self.prediction_duration_seconds:histogram = histogram('prediction_duration_seconds','Duration of prediction requests in seconds',buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0))
        self.prediction_active:Gauge = Gauge('prediction_active_requests','Number of active prediction requests')

    def start_server(self)->None:
        if not self._is_server_started:
            try:
                start_http_server(self.port, self.host)
                self._is_server_started = True
                print(f"Prometheus server started at http://{self.host}:{self.port}/metrics")
            except Exception as e:
                print(f"Failed to start Prometheus server: {e}")
                raise 
            
    def record_prediction(self,status:str)->None:
        self.prediction_request_counter.labels(status=status).inc() 
    def increment_active_prediction(self)->None:
        self.prediction_active.inc()
    def decrement_active_prediction(self)->None:
        self.prediction_active.dec()
        
    @contextmanager
    def track_prediction_duration(self)->Generator[None,None,None]:
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.prediction_duration_seconds.observe(duration)
            
promethus_service = PrometheusManager()