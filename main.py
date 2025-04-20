# import random
# import time
# import uuid
# import json
# import logging
# import threading
# from confluent_kafka import Producer
# from confluent_kafka.admin import AdminClient, NewTopic

# KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
# NUM_PARTITIONS = 4
# REPLICATION_FACTOR = 2
# TOPIC_NAME = 'financial_transactions'

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# producer_conf = {
#     'bootstrap.servers': KAFKA_BROKERS,
#     'queue.buffering.max.messages': 10000,
#     'queue.buffering.max.kbytes': 512000,
#     'batch.num.messages': 1000,
#     'linger.ms': 10,
#     'acks': 1,
#     'compression.type': 'gzip'
# }

# producer = Producer(producer_conf)

# stop_event = threading.Event()

# def create_topic(topic_name):
#     admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
#     try:
#         topic_metadata = admin_client.list_topics(timeout=10)
#         if topic_name not in topic_metadata.topics:
#             new_topic = NewTopic(
#                 topic=topic_name,
#                 num_partitions=NUM_PARTITIONS,
#                 replication_factor=REPLICATION_FACTOR,
#             )
#             fs = admin_client.create_topics([new_topic])
#             for topic, future in fs.items():
#                 try:
#                     future.result()
#                     logger.info(f"Topic '{topic_name}' created successfully!")
#                 except Exception as e:
#                     logger.error(f"Failed to create topic '{topic_name}': {e}")
#         else:
#             logger.info(f"Topic '{topic_name}' already exists.")
#     except Exception as e:
#         logger.error(f"Error listing topics: {e}")

# def delivery_report(err, msg):
#     if err is not None:
#         print(f'Delivery failed for record {msg.key()}: {err}')
#     else:
#         print(f'Record {msg.key()} successfully produced to topic {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

# def generate_transactions():
#     return dict(
#         transactionId=str(uuid.uuid4()),
#         userId=f"user_{random.randint(1, 100)}",
#         amount=round(random.uniform(50000, 150000), 2),
#         transactionTime=int(time.time()),
#         merchantId=random.choice(['merchant_1', 'merchant_2', 'merchant_3']),
#         transactionType=random.choice(['purchase', 'refund']),
#         location=f'location_{random.randint(1, 50)}',
#         paymentMethod=random.choice(['credit_card', 'paypal', 'bank_transfer']),
#         isInternational=random.choice(['True', 'False']),
#         currency=random.choice(['USD', 'EUR', 'GBP'])
#     )

# def produce_transaction(thread_id):
#     while not stop_event.is_set():
#         transaction = generate_transactions()
#         try:
#             producer.produce(
#                 topic=TOPIC_NAME,
#                 key=str(transaction['userId']),
#                 value=json.dumps(transaction).encode('utf-8'),
#                 on_delivery=delivery_report
#             )
#             print(f'Thread {thread_id} - Produced transaction: {transaction}')
#             producer.flush()
#             time.sleep(1)
#         except Exception as e:
#             print(f'Error sending transaction: {e}')
#             time.sleep(5)

# def producer_data_in_parallel(num_threads):
#     threads = []
#     for i in range(num_threads):
#         thread = threading.Thread(target=produce_transaction, args=(i,))
#         thread.start()
#         threads.append(thread)
#     return threads

# if __name__ == "__main__":
#     create_topic(TOPIC_NAME)
#     threads = producer_data_in_parallel(3)

#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         print("\nCtrl+C detected. Shutting down gracefully...")
#         stop_event.set()
#         for thread in threads:
#             thread.join()
#         print("All threads stopped. Exiting.")
import random
import time
import uuid
import json
import logging
import threading
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from concurrent.futures import ThreadPoolExecutor

KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 8  # Increased partitions for better parallelism
REPLICATION_FACTOR = 2
TOPIC_NAME = 'financial_transactions'
BATCH_SIZE = 100  # Number of messages to batch together
NUM_THREADS = 10  # More threads for parallel generation
DELAY_BETWEEN_BATCHES = 0.05  # Reduced delay between batches (in seconds)

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Optimized producer configuration
producer_conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'queue.buffering.max.messages': 100000,  # Increased buffer size
    'queue.buffering.max.kbytes': 1024000,   # Increased buffer memory
    'batch.num.messages': 10000,            # Increased batch size
    'linger.ms': 5,                         # Reduced linger time
    'acks': 1,                              # Only wait for leader acknowledgment
    'compression.type': 'lz4',              # Faster compression than gzip
    'request.timeout.ms': 30000,            # Increased timeout
    'socket.keepalive.enable': True         # Keep connections alive
}

producer = Producer(producer_conf)
stop_event = threading.Event()

def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    try:
        topic_metadata = admin_client.list_topics(timeout=10)
        if topic_name not in topic_metadata.topics:
            new_topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR,
                config={'retention.ms': '86400000'}  # 24 hour retention
            )
            fs = admin_client.create_topics([new_topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f"Topic '{topic_name}' created successfully!")
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic_name}': {e}")
        else:
            logger.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"Error listing topics: {e}")

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Delivery failed: {err}')
    else:
        # Reduced logging for better performance - only log errors
        pass

def generate_transaction():
    # Pre-defined choices for faster random selection
    user_ids = [f"user_{i}" for i in range(1, 101)]
    merchant_ids = ['merchant_1', 'merchant_2', 'merchant_3', 'merchant_4', 'merchant_5']
    transaction_types = ['purchase', 'refund']
    locations = [f'location_{i}' for i in range(1, 51)]
    payment_methods = ['credit_card', 'paypal', 'bank_transfer', 'crypto', 'mobile_payment']
    currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD']
    
    return dict(
        transactionId=str(uuid.uuid4()),
        userId=random.choice(user_ids),
        amount=round(random.uniform(50000, 150000), 2),
        transactionTime=int(time.time() * 1000),  # milliseconds
        merchantId=random.choice(merchant_ids),
        transactionType=random.choice(transaction_types),
        location=random.choice(locations),
        paymentMethod=random.choice(payment_methods),
        isInternational=random.choice(['True', 'False']),
        currency=random.choice(currencies)
    )

def produce_batch(thread_id, batch_num):
    """Generate and produce a batch of transactions"""
    batch = [generate_transaction() for _ in range(BATCH_SIZE)]
    
    try:
        for transaction in batch:
            producer.produce(
                topic=TOPIC_NAME,
                key=str(transaction['userId']),
                value=json.dumps(transaction).encode('utf-8'),
                on_delivery=delivery_report
            )
        
        # Only poll/flush after the entire batch to improve throughput
        producer.poll(0)  # Non-blocking poll
        
        if batch_num % 10 == 0:  # Log every 10 batches to reduce overhead
            logger.info(f'Thread {thread_id} - Produced batch {batch_num} ({BATCH_SIZE} msgs)')
            
    except Exception as e:
        logger.error(f'Error sending batch: {e}')

def producer_thread(thread_id):
    """Thread function to continuously produce batches"""
    batch_num = 0
    while not stop_event.is_set():
        produce_batch(thread_id, batch_num)
        batch_num += 1
        
        # Small sleep to prevent complete CPU saturation
        time.sleep(DELAY_BETWEEN_BATCHES)

def flush_daemon():
    """Dedicated thread to periodically flush the producer"""
    while not stop_event.is_set():
        producer.flush(timeout=1.0)
        time.sleep(0.5)  # Flush twice per second

def start_producer_threads():
    threads = []
    
    # Start flush daemon thread
    flush_thread = threading.Thread(target=flush_daemon)
    flush_thread.daemon = True
    flush_thread.start()
    threads.append(flush_thread)
    
    # Start producer threads
    for i in range(NUM_THREADS):
        thread = threading.Thread(target=producer_thread, args=(i,))
        thread.start()
        threads.append(thread)
        
    return threads

def print_stats(start_time, stats_interval=5.0):
    """Periodically print statistics"""
    msg_count = 0
    last_count = 0
    
    while not stop_event.is_set():
        time.sleep(stats_interval)
        current_count = producer.flush(timeout=0.0)
        msg_count += current_count
        
        elapsed = time.time() - start_time
        rate = msg_count / elapsed
        interval_rate = (msg_count - last_count) / stats_interval
        
        logger.info(f"Stats: {msg_count} total messages, {rate:.2f} msg/sec overall, {interval_rate:.2f} msg/sec current")
        last_count = msg_count

if __name__ == "__main__":
    create_topic(TOPIC_NAME)
    
    logger.info(f"Starting producer with {NUM_THREADS} threads, batch size {BATCH_SIZE}")
    start_time = time.time()
    
    # Start stat printing thread
    stat_thread = threading.Thread(target=print_stats, args=(start_time,))
    stat_thread.daemon = True
    stat_thread.start()
    
    # Start producer threads
    threads = start_producer_threads()

    try:
        # Main thread just waits
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nShutting down gracefully...")
        stop_event.set()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=5.0)
            
        # Final flush before exit
        remaining = producer.flush(timeout=10.0)
        if remaining > 0:
            logger.warning(f"{remaining} messages remained unflushed")
            
        logger.info("Producer shutdown complete")