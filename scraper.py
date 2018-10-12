from datetime import datetime
import sqlite3
import time
from threading import Event, Thread

import requests


class RequestRunner(Thread):
    def __init__(self, db_file):
        Thread.__init__(self)
        self.stop_event = Event()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "accept": "application/json",
                "ocp-apim-subscription-key": "db21164ca2c9430db9349cccf570a0e2",
            }
        )
        self.db_file = db_file
        self.tzinfo = datetime.utcnow().astimezone().tzinfo

    def run(self):
        self.db_conn = sqlite3.connect(self.db_file)
        while not self.stop_event.wait(5):
            json = self.get_timing_frame()
            self.write_json_to_db(json)
        self.db_conn.close()

    def get_timing_frame(self):
        res = self.session.get("https://api.imsa.com/live-scoring-plus/v1/results.json")
        return res.text

    def write_json_to_db(self, json):
        with self.db_conn:
            timestamp = datetime.now(self.tzinfo).isoformat(timespec="seconds")
            self.db_conn.execute(
                "INSERT INTO requests VALUES (?, ?)", (timestamp, json)
            )


def set_up_tables(db_conn):
    with db_conn:
        db_conn.execute("DROP TABLE IF EXISTS requests")
        db_conn.execute("CREATE TABLE requests (timestamp TEXT, json TEXT)")


def count_rows(db_conn):
    return db_conn.execute("SELECT COUNT(*) FROM requests").fetchone()


if __name__ == "__main__":
    conn = sqlite3.connect("imsa-plm.db")
    set_up_tables(conn)

    runner = RequestRunner("imsa-plm.db")
    runner.start()
    time.sleep(20)
    runner.stop_event.set()
    time.sleep(6)

    print(count_rows(conn))

    conn.close()
