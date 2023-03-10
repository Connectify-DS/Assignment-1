from models import Message
import psycopg2
import sys
sys.path.append("..")
from config import *
import threading

class MessageDBMS:
    def __init__(self, conn, cur,lock):
        self.conn = psycopg2.connect(database = DATABASE, user = USER, password = PASSWORD, 
                                host = HOST, port = PORT)
        self.cur=self.conn.cursor()
        self.lock=threading.Lock()

    def create_table(self):
        try:
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS MESSAGES(
                ID SERIAL PRIMARY KEY NOT NULL,
                MESSAGE TEXT NOT NULL);
            """)

            self.conn.commit()
        except:
            self.conn.rollback()

    def add_message(self,message):
        self.lock.acquire()
        try:
            self.cur.execute("""
                INSERT INTO MESSAGES (MESSAGE) 
                VALUES (%s)
                RETURNING ID
            """,(message,))

            message_id=self.cur.fetchone()[0]
            
            self.conn.commit()
            self.lock.release()
            return message_id
        except:
            self.conn.rollback()
            self.lock.release()

    def get_message(self,message_id):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT * FROM MESSAGES
                WHERE ID = %s
            """,(message_id,))

            row=self.cur.fetchone()

            m= Message(
                    message=row[1]
                )
            self.lock.release()
            return m
        except:
            self.conn.rollback()
            self.lock.release()
