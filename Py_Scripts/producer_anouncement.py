from kafka import KafkaProducer
import json

# Set up Kafka producer
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker address
topic_name = 'Employee-Announcement'  # Replace with your Kafka topic name

producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convert values to JSON and then to bytes
)

# Send announcement messages to the Kafka topic
try:
    producer.send(topic_name, value="Welcome to Company XYZ!")
    producer.send(topic_name, value="Please remember to collect your laptop from the mobile clinic!")
    producer.send(topic_name, value="ANNOUNCEMENT: Stay safe and wash your hands.")

except KeyboardInterrupt:
    # Handle keyboard interrupt (Ctrl+C)
    print("Producer interrupted. Closing.")

finally:
    # Flush and close the producer to ensure all messages are sent
    producer.flush()
    producer.close()
