import json
import logging
import os
import random
import time
import signal
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from random import randint

from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker
from jsonschema import validate, ValidationError, FormatChecker

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(dotenv_path="../.env")

fake = Faker()

# JSON Schema for transaction validation
TRANSACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "number", "minimum": 1000, "maximum": 9999},
        "amount": {"type": "number", "minimum": 0.01, "maximum": 100000},
        "currency": {"type": "string", "pattern": "^[A-Z]{3}$"},
        "merchant": {"type": "string"},
        "timestamp": {
            "type": "string",
            "format": "date-time"
        },
        "location": {"type": "string", "pattern": "^[A-Z]{2}$"},
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1}
    },
    "required": ["transaction_id", "user_id", "amount", "currency", "timestamp", 'is_fraud']
}


class TransactionProducer():
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'bootstrap_servers = "kafka:9092"')
        self.topic = os.getenv('KAFKA_TOPIC', 'transactions')
        self.running = False

        #config

        self.producer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "transaction-producer",
            "compression.type": "gzip",
            "linger.ms": 5,
            "batch.size": 16384,
        }

        try:
            self.producer = Producer(self.producer_config)
            logger.info("Kafka Producer initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Producer: {str(e)}")
            raise e

        self.compromised_users = set(random.sample(range(1000, 9999), 50)) #0.5% of users for balanced data
        self.high_risk_merchants = ['QuickCash', 'GlobalDigital', 'FastMoneyX']
        self.fraud_pattern_weights = {
            'account_takeover': 0.4,  # 40% of fraud cases
            'card_testing': 0.3,  # 30% of fraud cases
            'merchant_collusion': 0.2,  # 20% of fraud cases
            'geo_anomaly': 0.1  # 10% of fraud cases
        }
        # Configure shutdown in case of ctrl c
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def validate_transaction(self, transaction: Dict[str, Any]) -> bool:
        """Validate transaction against JSON schema with date-time checking"""
        try:
            validate(
                instance=transaction,
                schema=TRANSACTION_SCHEMA,
                format_checker=FormatChecker()
            )
            return True
        except ValidationError as e:
            logger.error(f"Invalid transaction: {e.message}")
            return False

    def generate_transaction(self) -> Optional[Dict[str, Any]]:
        """Generate transaction with 1-2% fraud rate using realistic patterns"""
        transaction = {
            "transaction_id": fake.uuid4(),
            "user_id": randint(1000, 9999),
            "amount": round(fake.pyfloat(min_value=0.01, max_value=10000), 2),  # Reduced max to $10k
            "currency": "USD",
            "merchant": fake.company(),
            "timestamp": (datetime.now(timezone.utc) +
                          timedelta(seconds=random.randint(-300, 300))).isoformat(),
            "location": fake.country_code(),
            "is_fraud": 0  # Uncommented fraud field
        }
        # Realistic fraud patterns (1.5% base rate +/-0.5%)
        is_fraud = 0
        amount = transaction['amount']
        user_id = transaction['user_id']
        merchant = transaction['merchant']

        # Pattern 1: Account takeover (0.6% of total transactions)
        if user_id in self.compromised_users and amount > 500:
            if random.random() < 0.3:  # 30% chance of fraud in compromised accounts
                is_fraud = 1
                transaction['amount'] = random.uniform(500, 5000)  # Typical takeover amounts
                transaction['merchant'] = random.choice(self.high_risk_merchants)

        # Pattern 2: Card testing (0.45% of total)
        if not is_fraud and amount < 2.0:
        # Simulate rapid small transactions (every 10th user in testing mode)
            if user_id % 1000 == 0 and random.random() < 0.25:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(0.01, 2.0), 2)
                transaction['location'] = 'US'  # Consistent testing location

        # Pattern 3: Merchant collusion (0.3% of total)
        if not is_fraud and merchant in self.high_risk_merchants:
            if amount > 300 and random.random() < 0.15:
                is_fraud = 1
                transaction['amount'] = random.uniform(300, 1500)

        # Pattern 4: Geographic anomalies (0.15% of total)
        if not is_fraud:
            # Simulate location change without actual state tracking
            if user_id % 500 == 0 and random.random() < 0.1:
                is_fraud = 1
                transaction['location'] = random.choice(['CN', 'RU', 'NG'])  # High-risk countries

        # Baseline random fraud (0.1-0.3%)
        if not is_fraud and random.random() < 0.002:
            is_fraud = 1
            transaction['amount'] = random.uniform(100, 2000)

        # Ensure final fraud rate stays between 1-2%
        transaction['is_fraud'] = is_fraud if random.random() < 0.985 else 0

        # Validate modified transaction ( manual schema registry)
        if self.validate_transaction(transaction):
            return transaction

        return None


    # send transaction data to kafka topic
    def send_transaction(self)->bool:
        try:
            transaction = self.generate_transaction()
            if not transaction:
                return False
            self.producer.produce(topic=self.topic,
                                  key=transaction['transaction_id'],
                                  value=json.dumps(transaction),
                                  callback= self.delivery_report)
            self.producer.poll(0) #to trigger callbcks
            return True
        except Exception as e:
            logger.error(f"Failed to send transaction: {str(e)}")
            return False


    def run_continuous_production(self, interval: float=0.0):
        """Run continuous message prod with shutdown"""
        self.running = True
        logger.info("Starting producer for topic %s...", self.topic)
        try:
            while self.running:
                if self.send_transaction():
                    time.sleep(interval)
        finally:
            self.shutdown()

    def shutdown(self, signum=None, frame=None):
        """shutdown procedure"""
        if self.running:
            logger.info("Initiating shutdown...")
            self.running = False
            if self.producer:
                self.producer.flush(timeout=30)
                self.producer.close()
            logger.info("Producer stopped")


if __name__ == '__main__':
    producer = TransactionProducer()
    producer.run_continuous_production()

