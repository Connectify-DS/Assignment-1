from message_queue_system import MessageQueueSystem

class broker:
    def __init__(self, persistent, brokerID) -> None:
        self.mqs = MessageQueueSystem(persistent=persistent)
        self.brokerId = brokerID
    
    def add_partition(self, partitionID):
        self.mqs.create_topic(topic_name=partitionID)

    def 
