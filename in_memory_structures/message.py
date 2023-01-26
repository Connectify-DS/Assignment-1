class MessageTable:
    def __init__(self,message_table_entries={}):
        self.message_table_entries = message_table_entries
        self.last_id = 0

    def get_new_id_auto_inc(self):
        self.last_id += 1
        return self.last_id

    def get_message(self, message_id):
        if message_id in self.message_table_entries:
            return self.message_table_entries[message_id]

    def add_message(self, message):
        message_id = self.get_new_id_auto_inc()
        if message_id not in self.message_table_entries:
            self.message_table_entries[message_id] = Message(message)
        return message_id


class Message:
    def __init__(self, message):
        self.message = message
