from kafka import KafkaProducer
import json

# Set up Kafka producer
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker address
topic_name = 'Department_ABC-Message'  # Replace with your Kafka topic name

producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convert values to JSON and then to bytes
)

# Send announcement messages to the Kafka topic
try:
    producer.send(topic_name, value="Welcome to Department ABC!")
    producer.send(topic_name, value="This is ABC Department message, hooray!")

except KeyboardInterrupt:
    # Handle keyboard interrupt (Ctrl+C)
    print("Producer interrupted. Closing.")

finally:
    # Flush and close the producer to ensure all messages are sent
    producer.flush()
    producer.close()
