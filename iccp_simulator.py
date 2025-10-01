# ============================================================================
# ICCP SIMULATOR WITH ELASTIC APM TRACING - DISTRIBUTED TRACING FIXED
# ============================================================================
# Version: 2.3.2
# Date: 1 October 2025
# Changes from 2.3.1:
# - Fixed distributed tracing by manually propagating traceparent header
# - Backend-api can now continue the trace from simulator
# - Constructs W3C compliant traceparent: 00-{trace_id}-{parent_id}-01
# ============================================================================

import json
import time
import random
import os
from kafka import KafkaProducer
from datetime import datetime, timezone
import logging
from typing import Dict, Any
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================================
# ELASTIC APM INITIALIZATION - MUST BE FIRST
# ============================================================================
from elasticapm import Client
import elasticapm

# Initialize the APM Client (documented API)
apm_client = Client({
    'SERVICE_NAME': os.environ.get('ELASTIC_APM_SERVICE_NAME', 'iccp-simulator'),
    'SECRET_TOKEN': os.environ.get('ELASTIC_APM_SECRET_TOKEN'),
    'SERVER_URL': os.environ.get('ELASTIC_APM_SERVER_URL'),
    'ENVIRONMENT': os.environ.get('ELASTIC_APM_ENVIRONMENT', 'production'),
    'SERVICE_VERSION': '2.3.2',
    'VERIFY_SERVER_CERT': False,
    'TRANSACTION_SAMPLE_RATE': float(os.environ.get('ELASTIC_APM_TRANSACTION_SAMPLE_RATE', '1.0')),
    'SPAN_FRAMES_MIN_DURATION': '5ms',
    'TRANSACTION_MAX_SPANS': 100,
    'CAPTURE_BODY': 'off',
    'LOG_LEVEL': 'warning',
})

# Instrument kafka-python (documented API)
elasticapm.instrument()

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# ICCP SIMULATOR CLASS
# ============================================================================
class ICCPSimulator:
    """
    ICCP Protocol Simulator for New Zealand National Grid
    Generates realistic SCADA messages with full distributed tracing
    """
    
    def __init__(self):
        self.kafka_brokers = os.environ.get('KAFKA_BROKERS', 'localhost:9092')
        self.site_name = os.environ.get('SITE_NAME', 'auckland-penrose')
        self.pod_name = os.environ.get('POD_NAME', 'unknown')
        
        self.site_config = self.load_site_config()
        self.message_counter = 0
        self.producer = self.create_kafka_producer()
        
        logger.info("="*70)
        logger.info(f"ICCP Simulator v2.3.2 - {self.site_config['display_name']}")
        logger.info("="*70)
        logger.info(f"Site: {self.site_config['site_id']}")
        logger.info(f"Kafka: {self.kafka_brokers}")
        logger.info(f"APM: {os.environ.get('ELASTIC_APM_SERVER_URL', 'Not configured')}")
        logger.info(f"Sampling: {os.environ.get('ELASTIC_APM_TRANSACTION_SAMPLE_RATE', '1.0')}")
        logger.info(f"Pod: {self.pod_name}")
        logger.info(f"Distributed Tracing: ENABLED (manual traceparent)")
        logger.info("="*70)

    def load_site_config(self) -> Dict[str, Any]:
        """Load site-specific configuration"""
        sites_config = {
            "auckland-penrose": {
                "site_id": "AKL_PENROSE",
                "display_name": "Auckland Penrose 330kV",
                "lat": -36.8485,
                "lon": 174.7633,
                "customers": ["CONTACT_ENERGY", "MERCURY_ENERGY", "GENESIS_ENERGY"],
                "message_frequency": 1.5
            },
            "wellington-central": {
                "site_id": "WLG_CENTRAL",
                "display_name": "Wellington Central 220kV",
                "lat": -41.2865,
                "lon": 174.7762,
                "customers": ["MERCURY_ENERGY", "GENESIS_ENERGY"],
                "message_frequency": 2.0
            },
            "christchurch-addington": {
                "site_id": "CHC_ADDINGTON",
                "display_name": "Christchurch Addington 66kV",
                "lat": -43.5321,
                "lon": 172.6362,
                "customers": ["MERIDIAN_ENERGY", "CONTACT_ENERGY"],
                "message_frequency": 1.8
            },
            "huntly-power": {
                "site_id": "HUNTLY_POWER",
                "display_name": "Huntly Power Station",
                "lat": -37.5483,
                "lon": 175.0681,
                "customers": ["GENESIS_ENERGY"],
                "message_frequency": 0.8
            },
            "manapouri-power": {
                "site_id": "MANAPOURI_POWER",
                "display_name": "Manapouri Power Station",
                "lat": -45.5361,
                "lon": 167.1761,
                "customers": ["MERIDIAN_ENERGY"],
                "message_frequency": 1.0
            }
        }
        return sites_config.get(self.site_name, sites_config["auckland-penrose"])
    
    def create_kafka_producer(self) -> KafkaProducer:
        """Initialize Kafka producer with compression"""
        compression = os.environ.get('KAFKA_COMPRESSION_TYPE', 'gzip')
        compression_codec = None if compression.lower() == 'none' or not compression else compression
        logger.info(f"Kafka compression: '{compression_codec}'")
        
        return KafkaProducer(
            bootstrap_servers=self.kafka_brokers,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            retry_backoff_ms=1000,
            retries=5,
            acks='all',
            compression_type=compression_codec,
            linger_ms=10,
            batch_size=16384
        )
    
    def generate_status_point_message(self, customer: str) -> Dict[str, Any]:
        """Generate STATUS_POINT message (Circuit Breaker status)"""
        self.message_counter += 1
        
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'site_id': self.site_config['site_id'],
            'site_name': self.site_config['display_name'],
            'customer_id': customer,
            'message_type': 'STATUS_POINT',
            'iccp_association': f'{self.site_config["site_id"]}-{customer}-01',
            'data': {
                'point_id': f'CB_{random.choice(["330", "220", "110"])}_L{random.randint(1,4)}_STATUS',
                'point_name': f'Circuit Breaker {random.choice(["330kV", "220kV", "110kV"])} Line {random.randint(1,4)}',
                'value': random.choice([0, 1]),
                'quality': random.choices(['GOOD', 'UNCERTAIN', 'INVALID'], weights=[92, 6, 2])[0],
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'change_counter': random.randint(1000, 9999)
            },
            'location': {
                'lat': self.site_config['lat'],
                'lon': self.site_config['lon'],
                'region': self.site_config['site_id'].split('_')[0]
            },
            'metadata': {
                'protocol_version': 'IEC60870-6-503',
                'message_size': random.randint(128, 512),
                'association_active': random.choices([True, False], weights=[98, 2])[0],
                'roundtrip_time_ms': random.randint(5, 25),
                'message_number': self.message_counter
            }
        }
    
    def generate_analog_value_message(self, customer: str) -> Dict[str, Any]:
        """Generate ANALOG_VALUE message (Power measurements)"""
        self.message_counter += 1
        
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'site_id': self.site_config['site_id'],
            'site_name': self.site_config['display_name'],
            'customer_id': customer,
            'message_type': 'ANALOG_VALUE',
            'iccp_association': f'{self.site_config["site_id"]}-{customer}-01',
            'data': {
                'point_id': f'MW_{random.choice(["GEN", "LOAD", "FLOW"])}_L{random.randint(1,4)}',
                'point_name': f'{random.choice(["Generation", "Load", "Power Flow"])} MW Line {random.randint(1,4)}',
                'value': round(random.uniform(50.0, 500.0), 2),
                'quality': random.choices(['GOOD', 'UNCERTAIN', 'INVALID'], weights=[94, 5, 1])[0],
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'units': 'MW'
            },
            'location': {
                'lat': self.site_config['lat'],
                'lon': self.site_config['lon'],
                'region': self.site_config['site_id'].split('_')[0]
            },
            'metadata': {
                'protocol_version': 'IEC60870-6-503',
                'message_size': random.randint(128, 512),
                'association_active': True,
                'roundtrip_time_ms': random.randint(5, 25),
                'message_number': self.message_counter
            }
        }
    
    def generate_protection_event_message(self, customer: str) -> Dict[str, Any]:
        """Generate PROTECTION_EVENT message (Alarms)"""
        self.message_counter += 1
        
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'site_id': self.site_config['site_id'],
            'site_name': self.site_config['display_name'],
            'customer_id': customer,
            'message_type': 'PROTECTION_EVENT',
            'iccp_association': f'{self.site_config["site_id"]}-{customer}-01',
            'data': {
                'event_id': f'PROT_EVT_{random.randint(10000, 99999)}',
                'event_type': random.choice(['OVERCURRENT', 'UNDERVOLTAGE', 'FREQUENCY_DEVIATION', 'LINE_FAULT']),
                'severity': random.choices(['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'], weights=[40, 35, 20, 5])[0],
                'cleared': random.choices([True, False], weights=[85, 15])[0],
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'equipment_affected': f'Line {random.randint(1,4)}'
            },
            'location': {
                'lat': self.site_config['lat'],
                'lon': self.site_config['lon'],
                'region': self.site_config['site_id'].split('_')[0]
            },
            'metadata': {
                'protocol_version': 'IEC60870-6-503',
                'message_size': random.randint(200, 600),
                'association_active': True,
                'roundtrip_time_ms': random.randint(8, 30),
                'message_number': self.message_counter
            }
        }

    def send_message_with_tracing(self, message: Dict[str, Any]):
        """
        Send message to Kafka with full distributed tracing.
        Manually constructs and propagates W3C traceparent header.
        """
        message_type = message['message_type']
        topic_map = {
            'STATUS_POINT': 'iccp-status-points',
            'ANALOG_VALUE': 'iccp-analog-values', 
            'PROTECTION_EVENT': 'iccp-protection-events',
            'ENERGY_ACCOUNTING': 'iccp-energy-accounting'
        }
        topic = topic_map.get(message_type, 'iccp-status-points')
        
        # ═══════════════════════════════════════════════════════════════════
        # Start APM transaction using Client API (documented)
        # Reference: Client.begin_transaction(transaction_type)
        # ═══════════════════════════════════════════════════════════════════
        apm_client.begin_transaction('messaging')
        
        try:
            # Set transaction name (documented)
            # Reference: elasticapm.set_transaction_name(name, override)
            elasticapm.set_transaction_name(f'produce {topic}', override=True)
            
            # Add labels for filtering (documented)
            # Reference: elasticapm.label(**kwargs)
            elasticapm.label(
                site_id=message['site_id'],
                site_name=message['site_name'],
                customer=message['customer_id'],
                message_type=message_type,
                topic=topic,
                pod=self.pod_name
            )
            
            # ═══════════════════════════════════════════════════════════════
            # CRITICAL: Manually construct W3C traceparent header
            # Format: 00-{trace_id}-{parent_id}-{flags}
            # This enables backend-api to continue the trace
            # 
            # Reference: elasticapm.get_trace_id(), elasticapm.get_transaction_id()
            # W3C Spec: https://www.w3.org/TR/trace-context/#traceparent-header
            # ═══════════════════════════════════════════════════════════════
            trace_id = elasticapm.get_trace_id()
            transaction_id = elasticapm.get_transaction_id()
            
            kafka_headers = []
            if trace_id and transaction_id:
                # Construct W3C compliant traceparent
                # 00 = version, 01 = sampled flag
                traceparent = f"00-{trace_id}-{transaction_id}-01"
                kafka_headers = [('traceparent', traceparent.encode('utf-8'))]
                logger.debug(f"✓ Propagating trace: {trace_id[:16]}... -> {topic}")
            else:
                logger.warning(f"⚠ No trace context for {topic} - distributed tracing will break")
            
            # ═══════════════════════════════════════════════════════════════
            # Create span using context manager (documented)
            # Reference: elasticapm.capture_span(name, span_type, span_subtype, 
            #                                     span_action, labels)
            # ═══════════════════════════════════════════════════════════════
            with elasticapm.capture_span(
                name=f'send to {topic}',
                span_type='messaging',
                span_subtype='kafka',
                span_action='send',
                labels={
                    'topic': topic,
                    'message_type': message_type,
                    'message_number': str(message['metadata']['message_number'])
                }
            ):
                # Send message WITH traceparent header for distributed tracing
                future = self.producer.send(
                    topic,
                    value=message,
                    headers=kafka_headers  # ← THIS IS CRITICAL FOR DISTRIBUTED TRACING
                )
                
                # Wait for send confirmation
                record_metadata = future.get(timeout=10)
                
                # Log successful send
                logger.info(
                    f"✓ {message_type} → {topic} "
                    f"(p:{record_metadata.partition}, o:{record_metadata.offset}) "
                    f"#{message['metadata']['message_number']}"
                )
            
            # Mark transaction as successful (documented)
            # Reference: elasticapm.set_transaction_result(result, override)
            #            elasticapm.set_transaction_outcome(outcome, override)
            elasticapm.set_transaction_result('success', override=True)
            elasticapm.set_transaction_outcome('success', override=True)
                    
        except Exception as e:
            logger.error(f"✗ Send failed: {e}")
            
            # Mark transaction as failed (documented)
            elasticapm.set_transaction_result('error', override=True)
            elasticapm.set_transaction_outcome('failure', override=True)
            
            # Capture exception (documented)
            # Reference: Client.capture_exception()
            apm_client.capture_exception()
            
            raise
            
        finally:
            # End transaction (documented)
            # Reference: Client.end_transaction(name, result)
            apm_client.end_transaction(f'produce {topic}', 'success')
    
    def generate_and_send_message(self):
        """Generate random message and send with tracing"""
        customer = random.choice(self.site_config['customers'])
        message_types = ['STATUS_POINT', 'ANALOG_VALUE', 'PROTECTION_EVENT', 'ENERGY_ACCOUNTING']
        weights = [50, 30, 15, 5]
        message_type = random.choices(message_types, weights=weights)[0]
        
        # Generate appropriate message type
        if message_type == 'STATUS_POINT':
            message = self.generate_status_point_message(customer)
        elif message_type == 'ANALOG_VALUE':
            message = self.generate_analog_value_message(customer)
        elif message_type == 'PROTECTION_EVENT':
            message = self.generate_protection_event_message(customer)
        else:
            message = self.generate_status_point_message(customer)
        
        # Send with full APM tracing
        self.send_message_with_tracing(message)
    
    def run_simulation(self):
        """Main simulation loop"""
        logger.info(f"🚀 Starting ICCP simulation loop")
        logger.info(f"Message frequency: {self.site_config['message_frequency']}s ± 0.3s")
        logger.info(f"Distributed tracing: traceparent headers enabled")
        
        message_count = 0
        start_time = time.time()
        
        while True:
            try:
                self.generate_and_send_message()
                message_count += 1
                
                # Log throughput stats every 100 messages
                if message_count % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = message_count / elapsed
                    logger.info(f"📊 Sent {message_count} messages ({rate:.1f} msg/sec)")
                
                # Sleep with jitter
                sleep_time = self.site_config['message_frequency'] + random.uniform(-0.3, 0.3)
                time.sleep(max(0.5, sleep_time))
                
            except KeyboardInterrupt:
                logger.info("🛑 Simulation stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Error in simulation loop: {e}")
                try:
                    apm_client.capture_exception()
                except:
                    pass
                time.sleep(5)
        
        # Cleanup
        self.producer.flush()
        self.producer.close()
        
        elapsed = time.time() - start_time
        rate = message_count / elapsed if elapsed > 0 else 0
        logger.info("="*70)
        logger.info(f"✅ ICCP Simulator shutdown complete")
        logger.info(f"Total messages: {message_count}")
        logger.info(f"Average rate: {rate:.1f} msg/sec")
        logger.info(f"Uptime: {elapsed:.0f} seconds")
        logger.info("="*70)

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
def main():
    """Main entry point"""
    logger.info("")
    logger.info("╔" + "═"*68 + "╗")
    logger.info("║" + " "*68 + "║")
    logger.info("║" + "  ICCP SIMULATOR v2.3.2 - Distributed Tracing Fixed".center(68) + "║")
    logger.info("║" + "  Manual W3C Traceparent Propagation".center(68) + "║")
    logger.info("║" + " "*68 + "║")
    logger.info("╚" + "═"*68 + "╝")
    logger.info("")
    
    try:
        simulator = ICCPSimulator()
        simulator.run_simulation()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        try:
            apm_client.capture_exception()
        except:
            pass
        raise

if __name__ == "__main__":
    main()