# Microservices & Machine Learning Architecture with RabbitMQ

This repository is built to demonstrate how machine learning models can be integrated into a microservices architecture asynchronously without causing system bottlenecks under high traffic conditions.

Instead of using copy-paste "vibe coding" patterns, all messaging infrastructures and abstraction layers have been crafted line-by-line from scratch to truly grasp the underlying low-level mechanics.

## 🛠️ Tech Stack
* **Python:** For data preprocessing, model inference, and queue management.
* **RabbitMQ:** As the Message Broker for asynchronous inter-service communication and task distribution.
* **Docker:** To run an isolated and easily portable RabbitMQ container.
* **Pika:** The standard library bridging Python and RabbitMQ with flexible integration.

## 📐 Architectural Design & Roadmap

1. **Centralized Infrastructure Management (Done):** Developed a robust `RabbitMQManager` class implementing a Singleton Connection pattern. It provides auto-recovery for channels and decouples queue/exchange names from the publisher logic for maximum flexibility.
2. **Reliable Messaging (Done):** Configured manual message acknowledgments (`auto_ack=False`) to achieve 100% data durability. If a worker crashes mid-task, RabbitMQ safely re-queues the message instead of losing it.
3. **ML Model Integration (Next):** Pre-trained machine learning models (utilizing Pandas and CatBoost) will be wrapped inside background Worker (Consumer) services. Upcoming inference requests (`predict`) will be consumed and processed out of the queue asynchronously.
