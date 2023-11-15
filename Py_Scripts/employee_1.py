from kafka import KafkaConsumer

# Set up Kafka consumer
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker address
topic_name = 'Employee-Announcement'  # Replace with your Kafka topic name

consumer = KafkaConsumer(
    topic_name,
    group_id='employee1',
    bootstrap_servers=[bootstrap_servers],
    auto_offset_reset='earliest',   # Start from the beginning
    enable_auto_commit=False,  # Disable automatic offset committing
)

# Continuously poll for new messages
try:
    for message in consumer:
        # Process the received message value
        print(f"Received message: {message.value.decode('utf-8')}")

        # Manually commit the offset to mark the message as processed
        consumer.commit()

except KeyboardInterrupt:
    # Handle keyboard interrupt (Ctrl+C)
    print("Consumer interrupted. Closing.")

finally:
    # Close the consumer to release resources
    consumer.close()
