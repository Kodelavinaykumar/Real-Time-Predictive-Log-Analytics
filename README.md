# Real-Time-Predictive-Log-Analytics
from pykafka import KafkaClient
from hdfs import InsecureClient
import time
import os
# Kafka broker address and topic
kafka_broker = 'localhost:9092'
kafka_topic = 'log_java'
# HDFS configuration
hdfs_client = InsecureClient('http://localhost:9870', user='Dharani')
hdfs_path = '/user/log/'
try:
 # Create a Kafka client and consumer
 client = KafkaClient(hosts=kafka_broker)
 topic = client.topics[kafka_topic]
 consumer = topic.get_simple_consumer()
 # Buffer for accumulating data
 data_buffer = []
 while True:
 message = consumer.consume(block=False)
 if message is not None:
 try:
 # Kafka messages are plain text log data
 log_data = message.value.decode('utf-8')
 data_buffer.append(log_data)
 # Print the consumed message (for debugging purposes)
 print(f"Consumed message: {log_data}")
 # Write buffered data to HDFS in chunks
 if len(data_buffer) >= 10: # Adjust the buffer size as needed
 hdfs_client.write(
 os.path.join(hdfs_path, 'log.txt'),
 "\n".join(data_buffer) + "\n",
 append=True
 )
 data_buffer.clear()
 except Exception as e:
 # Log the error and continue
print(f"An error occurred: {str(e)}")
 continue
 # Control the consumption rate
 time.sleep(1)
except Exception as e:
 print(f"An error occurred: {str(e)}")
finally:
 # Clean up resources
 consumer.stop()
 hdfs_client.close()
 print("Shutting down the script gracefully")
