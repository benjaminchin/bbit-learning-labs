from consumer_interface import mqConsumerInterface
import pika
import os
class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key, exchange_name, queue_name) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection


    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        self.channel = self.connection.channel()
        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name)
        self.channel.queue_declare(queue= self.queue_name)
        
        self.channel.queue_bind(
        queue= self.queue_name,
        routing_key= self.binding_key,
        exchange= self.exchange_name ,
        )
        self.channel.basic_consume(
        self.queue_name , on_message_callback, auto_ack=False
        )

        def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge message
            channel.basic_ack(method_frame.delivery_tag, False)
            print(body)
        #Print message (The message is contained in the body parameter variable)

        

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print("[*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()
        # Start consuming messages
        
    
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        self.channel.close()
        self.connection.close()
        
        


    

