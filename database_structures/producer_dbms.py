import psycopg2

from models import Producer

class ProducerDBMS:
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur=cur

    def create_table(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS PRODUCERS(
            ID SERIAL PRIMARY KEY NOT NULL,
            TOPIC TEXT NOT NULL);
        """)

        self.conn.commit()

    def register_new_producer_to_topic(self,topic_name):
        self.cur.execute("""
            INSERT INTO PRODUCERS (TOPIC) 
            VALUES (%s)
            RETURNING ID
        """,(topic_name,))

        producer_id=self.cur.fetchone()[0]
        
        self.conn.commit()

        return producer_id

    def get_producer(self,producer_id):
        self.cur.execute("""
            SELECT * FROM PRODUCERS
            WHERE ID = %s
        """,(producer_id,))

        try:
            row=self.cur.fetchone()
            if row is None:
                raise Exception("Invalid Producer Id")
        except Exception as e:
            raise e
        
        return Producer(
                producer_id=row[0],
                producer_topic=row[1]
            )

if __name__=="__main__":
    dbms=ProducerDBMS()
    dbms.create_table()