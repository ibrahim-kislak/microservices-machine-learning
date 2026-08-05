import os
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

class JaegerManager:
    def __init__(self,service_name:str):
        self.service_name = service_name
        jaeger_host = os.getenv("JAEGER_HOST", "localhost")
        
        resource = Resource.create(attributes={"service.name": self.service_name})
        provider = TracerProvider(resource=resource)
        
        otlp_exporter=OTLPSpanExporter(endpoint=f"http://{jaeger_host}:4318/v1/traces")
        processor = BatchSpanProcessor(otlp_exporter)
        provider.add_span_processor(processor)
        
        trace.set_tracer_provider(provider)
        self.tracer = trace.get_tracer(service_name)
        self.propagator = TraceContextTextMapPropagator()
        
        def inject_context(carrier,headers:dict)->dict:
            if headers is None:
                headers = {}
            self.propagator.inject(carrier, headers)
            return headers
        def extract_context(carrier,headers:dict)->dict:
            if headers is None:
                headers = {}
            context = self.propagator.extract(carrier=headers)
            return context
        