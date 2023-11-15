from kafka import KafkaConsumer, TopicPartition

# Set up Kafka consumer
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker address
topic_name = 'quickstart-events'  # Replace with your Kafka topic name
partition_number = 0  # Replace with the desired partition number

consumer = KafkaConsumer(
    bootstrap_servers=[bootstrap_servers],
    group_id='employee_3',  # You can set a unique consumer group if needed
    enable_auto_commit=False  # Disable automatic offset committing
)

# Assign the consumer to the specific partition
partition = TopicPartition(topic_name, partition_number)
consumer.assign([partition])

# Seek to the beginning of the partition (you can adjust this based on your needs)
consumer.seek_to_beginning(partition)

# Continuously poll for messages from the specified partition
try:
    for message in consumer:
        print(f"Received message from partition {message.partition}: {message.value.decode('utf-8')}")

except KeyboardInterrupt:
    # Handle keyboard interrupt (Ctrl+C)
    print("Consumer interrupted. Closing.")

finally:
    # Close the consumer to release resources
    consumer.close()
