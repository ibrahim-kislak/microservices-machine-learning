# Stroke Prediction Pipeline: Event-Driven Microservices Architecture

## Overview
This repository contains a robust, fully containerized microservices architecture designed for real-time medical data processing and stroke risk prediction. Built upon an event-driven paradigm, the system seamlessly integrates an asynchronous message broker, in-memory caching, and a pre-trained machine learning model to evaluate patient metrics and determine medical risk levels.

The architecture is strictly modular, ensuring high scalability and fault tolerance across discrete service boundaries. 

## Technologies and Frameworks
* **Language:** Python 3.10+
* **Containerization:** Docker & Docker Compose
* **Message Broker:** RabbitMQ (AMQP protocol)
* **In-Memory Datastore:** Redis
* **Machine Learning:** H2O.ai (GLM / AutoML)
* **Libraries:** `pika`, `redis`, `pandas`, `json`

## System Architecture and Services
The pipeline consists of three primary microservices communicating over a shared Docker bridge network.

### 1. Publisher Service
* **Function:** Acts as the entry point for patient data streams.
* **Mechanism:** Before dispatching data, the service queries the Redis cache using a unique identifier (`stroke_prediction:{patient_id}`). 
* **Routing:** If a cache miss occurs, the patient payload is published to the RabbitMQ `prediction_exchange` (Topic Exchange) using the routing key `hospital.stroke.prediction`. A cache record is then set with a Time-To-Live (TTL) of 360 seconds to prevent redundant processing.

### 2. Prediction Service
* **Function:** The core inference engine of the pipeline.
* **Mechanism:** Subscribes to the `prediction_queue` bound to the `hospital.*.prediction` routing key. 
* **Processing:** Parses incoming JSON payloads, converts them into an `H2OFrame`, and executes inference using a pre-loaded H2O Generalized Linear Model (GLM).
* **Thresholding:** Utilizes a statistically optimized classification threshold of `0.104433`. This lower threshold is a deliberate architectural decision designed to maximize **Recall (Sensitivity)**, minimizing false negatives which are highly critical in imbalanced medical datasets.

### 3. Logging Service
* **Function:** Acts as a centralized auditing node.
* **Mechanism:** Subscribes to the `logging_queue` which is bound to the wildcard routing key `hospital.#`. This ensures that all message traffic traversing the exchange is captured, logged, and acknowledged without interfering with the primary prediction workflow.

## Key Achievements
* **Decoupled Architecture:** Successfully implemented an event-driven topology where services operate independently, connected only by AMQP exchanges.
* **Resource Optimization:** Integrated a Redis caching layer that prevents identical payloads from unnecessarily consuming CPU cycles during the ML inference phase.
* **Medical Data Adaptation:** Configured the inference pipeline to handle highly imbalanced healthcare data by adjusting probability thresholds to prioritize patient safety over raw accuracy.

## Technical Challenges & Resolutions
During the development and orchestration of this pipeline, several critical challenges were addressed:

1. **Docker Network Resolution:** 
   * *Challenge:* Services failing to connect to the datastore with `Connection Refused (111)` errors due to referencing `localhost`.
   * *Resolution:* Reconfigured the `RedisManager` class and environment variables to utilize internal Docker DNS, pointing directly to the `redis` container hostname.
2. **Asynchronous Race Conditions:** 
   * *Challenge:* Publisher and Consumer services crashing upon boot because the RabbitMQ broker had not fully initialized its TCP sockets.
   * *Resolution:* Implemented startup delays and health checks within the Docker Compose configuration to ensure the broker is fully ready to accept connections before dependent services boot.
3. **Data Type Mismatches in ML Inference:** 
   * *Challenge:* `TypeError` exceptions (`list indices must be integers or slices, not str`) occurring when extracting probability values from H2O models.
   * *Resolution:* Bypassed unstable Pandas conversions by directly indexing the native `H2OFrame` object (`predict["p1"][0, 0]`), ensuring type safety and improving execution speed.

## Future Roadmap
The architecture is designed with extensibility in mind. Upcoming iterations will focus on observability and modeling enhancements:

* **Prometheus Integration:** Exposing internal service metrics (message processing rates, cache hit/miss ratios, inference latency) for time-series monitoring.
* **Grafana Dashboards:** Building comprehensive visualization layers on top of Prometheus data to monitor system health in real-time.
* **Grafana Loki:** Transitioning from standard container output to centralized, label-based log aggregation for advanced querying and distributed tracing.
* **Model Expansion:** Deploying a multi-model architecture (incorporating frameworks such as LightGBM, CatBoost, or Neural Networks) to run concurrent inferences and establish an ensemble voting mechanism.

## Usage
To provision and start the entire architecture, execute the following command in the repository root:
```bash
docker compose up -d --build