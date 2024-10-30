# # 1.0.dev304.1

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from confluent_kafka.admin import AdminClient, NewTopic

import asyncio
import logging
import time
from app.loggers import logger_k          # Import
# logger_k = logging.getLogger()  # Declare
# logger_k.setLevel(logging.DEBUG)  # Declare
    

class KafkaAio():
    def __init__(self, config_basic: dict | None = None, 
                 config_aio_producer: dict | None = None,
                 config_aio_consumer: dict | None = None) -> None:
        self._adminclient         = None
        self._producer           = None
        # self._producer_avro      = None

        self.config_basic = config_basic or {}
        self.config_aio_producer = config_aio_producer or {}
        self.config_aio_consumer = config_aio_consumer or {}
        # self.config_aio = config_aio or {}
        

    # Async
    ##############################
    async def connect_and_start(self):
        """Config and start the producers
        TODO: wait for aiokafkaMAJ to have an async admin client, only one interface/config will be needed
        
        Args:
            basicconfig (dict): config dictionnary for Kafka Admin client
            aioconfig (dict): config dictionnary for asynchronous Kafka Producers
        """
        try:
            self._adminclient       = AdminClient(self.config_basic)
            self._producer          = AIOKafkaProducer(**self.config_aio_producer)
            # self._producer_avro      = AIOKafkaProducer(**self.config_aio_producer, value_serializer=self.avro_serializer)
            # self._producer_protobuf  = AIOKafkaProducer(**aioconfig, value_serializer=self.protobuf_serializer)

            self._consumer          = AIOKafkaConsumer(
                                        self.config_aio_consumer["topic"],
                                        auto_offset_reset='earliest',
                                        enable_auto_commit=False,
                                        **self.config_aio_consumer["config"])

        except:
            raise ValueError(
                f"Kafka AdminClient and Producers failed, check configs : config_basic={self.config_aio_producer} and config_basic={self.config_basic}"
            )



        await self._producer.start()
        await self._consumer.start()
        # await self._producer_avro.start()
        # await self._adminclient.stop()

    async def start(self):
        await self._producer.start()
        await self._consumer.start()
        # await self._producer_avro.start()
        # await self._adminclient.stop()

    async def stop(self):
        await self._producer.stop()
        await self._consumer.stop()
        # await self._producer_avro.stop()
        # await self._adminclient.stop()


    # Admin
    ##############################
        """
        from: https://www.confluent.io/blog/kafka-python-asyncio-integration/
        'Unlike the Producer, the Python AdminClient uses futures to communicate the outcome of requests back to the application 
        (though a poll loop is still required to handle log, error, and statistics events). These are not asyncio Futures though. 
        Rather, they are of type concurrent.futures. Future. In order to await them in your asyncio application, 
        you’ll need to convert them using the asyncio.wrap_future method.'

        Adminclient will be correctly integrated in next aiokafka release, wait for it instead of dev/monkeypatch
            + Current behavior and tests doesn't hold good async behaviour ;
            + Tests should be refactored.
        """


    async def healthcheck(self):
        """Healtcheck to ensure connection with a Kafka Cluster
        Actually, a simple topic listing ; which is a discount healtcheck
        TODO: external monitoring (because there should be multiple brokers so zookeeper ?) Another function ? 
 <
        Returns:
            bool: True for connection success
        """
        logger_k.debug(f"HEALTH CHECK ")
        if self._adminclient.list_topics() is not None:
            topi = self._adminclient.list_topics()
            logger_k.debug(f"HEALTHY TOPICS : {topi.topics}")
            return True
        else:
            return False

    # CRUD Topic
    ##############################
    def create_topic(self, topic_name, partitions=10, replication_factor=1):
        """Simple wrapper for Kafka Topic Creation, partitions and replication factors 
        are to be overriden following https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/

        Args:
            topic_name (str): the name of the Kafka topic to be created
            partitions (int, optional): number of partitions (parallelisme units) for the Kafka topic. Defaults to 10.
            replication_factor (int, optional): number of replications for the Kafka topic. Defaults to 1.

        Returns:
            ...: returns a future, hardly awaitable, see TODO
        """
        logger_k.info(f"CREATING topic {topic_name}")
        new_topic = NewTopic(topic=topic_name, num_partitions=partitions, replication_factor=replication_factor)
        return self.adminclient.create_topics([new_topic])

    def delete_topic(self, topic_name):
        """Simple wrapper for Kafka Topic Deletion
        Args:
            topic_name (str): the name of the Kafka topic to be deleted
        Returns:
            ...: returns a future, hardly awaitable, see TODO
        """
        logger_k.info(f"DELETING topic {topic_name}")
        # new_topic = NewTopic(topic=topic_name, num_partitions=partitions, replication_factor=replication_factor)
        return self._adminclient.create_topics([topic_name])

    # Producer
    ##############################
    async def produce_message(self, topic: str, key: bytes, value: bytes):
        """Simple wrapper for Kafka Producer
        note: override the send_and_wait to avoid the unreadable ValueError due to async or bad bytes format

        Args:
            topic (str): the Kafka topic name, aimed for message production
            key (bytes): Kafka message key
            value (bytes): Kafka message value

        Returns:
            _type_: _description_
        """
        try:
            logger_k.info(f"AIOProducing message {key}:{value} to topic {topic}")
            return await self._producer.send_and_wait(topic=topic, key=key, value=value)
        except BaseException as e:
            # print(e.message, e.args)
            print(e)

    async def produce_message_str(self, topic: str, key: str, value: str):
        """Simple wrapper for Kafka Producer
        note: override the send_and_wait to avoid the unreadable ValueError due to async or bad bytes format

        Args:
            topic (str): the Kafka topic name, aimed for message production
            key (str): Kafka message key, encoded in utf-8
            value (str): Kafka message value, encoded in utf-8

        Returns:
            _type_: _description_
        """
        try:
            logger_k.info(f"AIOProducing message {key}:{value} to topic {topic}")
            return await self._producer.send_and_wait(topic=topic, key=key.encode('utf-8'), value=value.encode('utf-8'))
        except BaseException as e:
            # print(e.message, e.args)
            print(e)

    # async def produce_message_record(self, topic: str, reco: InlkRecord):
    #     try:
    #         logger_k.info(f"AIOProducing message {reco.in_schema.name}:{reco.content} to topic {topic}")
    #         return await self._producer.send_and_wait(topic=topic, key=(reco.in_schema.name).encode('utf-8'), value=(reco.content).encode('utf-8'))
    #     except BaseException as e:
    #         print(e.message, e.args)

    async def produce_avro_serialized(self):
        pass


    # Consumer
    ##############################
    async def consume(self, wait_for_msg_timeout: int = 5 ):
        await self._consumer.start()
        try:
            async with asyncio.timeout(10):
                msg = await self._consumer.getone()
        except TimeoutError:
            print("The long operation timed out, but we've handled it.")
        
        await self._consumer.commit()
        await self._consumer.stop()
        return msg

    async def consume_key(self, key_to_wait_for:str, wait_for_msg_timeout: int = 5, sleep_time: float = 0.5 ):
        async def consumomatic(key_to_wait_for:str):
            while True:
                msg = await self._consumer.getone()
                msg_key = bytes.decode(msg.key, encoding='utf-8')
                if msg_key == key_to_wait_for:
                    logger_k.debug(f"OUI msg_key={msg_key}")
                    break
                else: 
                    logger_k.debug(f"NON msg_key={msg_key}")
                    time.sleep(sleep_time)
            return msg
       
        # await self._consumer.start()
                
        try:
            msg = await asyncio.wait_for(consumomatic(key_to_wait_for), timeout=wait_for_msg_timeout) 
        except TimeoutError as te:
            print("The long operation timed out, but we've handled it.")
            logger_k.error(te)
            logger_k.error(f"consume_key has timed out with key_to_wait_for={key_to_wait_for}")
            await self._consumer.commit()
            # await self._consumer.stop()
            raise te

        await self._consumer.commit()
        # await self._consumer.stop()
        
        return msg

    

    # Serialization
    ##############################
    def avro_serializer(self, value):
        return value


    