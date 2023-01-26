import psycopg2

from in_memory_structures import Consumer

class ConsumerDBMS:
    def __init__(self):
        self.conn = psycopg2.connect(database = "mqsdb", user = "postgres", password = "mayank", 
                                host = "127.0.0.1", port = "5432")
        self.cur=self.conn.cursor()

    def create_table(self):
        self.cur.execute("""
            CREATE TABLE CONSUMERS
            ID INT PRIMARY KEY NOT NULL,
            TOPIC TEXT NOT NULL,
            OFFSET INT NOT NULL,
        """)

        self.conn.commit()

    def create_consumer(self,topic_name):
        self.cur.execute("""
            INSERT INTO CONSUMERS (TOPIC,OFFSET) 
            VALUES (%s,0)
        """,topic_name)

        id=self.cur.fetchone()[0]
        
        self.conn.commit()

        return id

    def select_consumer(self,consumer_id):
        self.cur.execute("""
            SELECT * FROM CONSUMERS
            WHERE ID = %s
        """,consumer_id)

        row=self.cur.fetchone()

        return Consumer(
                consumer_id=row[0],
                topic_name=row[1],
                cur_topic_queue_offset=row[2]
            )

if __name__=="__main__":
    dbms=ConsumerDBMS()
    dbms.create_table()