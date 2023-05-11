import pymysql
import requests
import json
from datetime import datetime, timedelta

QUERIES = {
    "mad": {
        "update_stop": "UPDATE pokestop SET name = %s, image = %s WHERE pokestop_id = %s",
        "update_gym": "UPDATE gymdetails SET name = %s, url = %s WHERE gym_id = %s",
        "empty_gyms": (
            "SELECT gym.gym_id FROM gym LEFT JOIN gymdetails on gym.gym_id = gymdetails.gym_id "
            "WHERE name = 'unknown'"
        ),
        "empty_stops": "SELECT pokestop_id FROM pokestop WHERE name IS NULL"
    },
    "rdm": {
        "update_stop": "UPDATE pokestop SET name = %s, url = %s WHERE id = %s",
        "update_gym": "UPDATE gym SET name = %s, url = %s WHERE id = %s",
        "empty_gyms": "SELECT id FROM gym WHERE name IS NULL",
        "empty_stops": "SELECT id FROM pokestop WHERE name IS NULL"
    }
}


class Queries():
    def __init__(self, config):
        self.connection = pymysql.connect(
            host=config.db_host,
            user=config.db_user,
            password=config.db_password,
            database=config.db_name_portal,
            port=config.db_port,
            autocommit=True
        )
        self.cursor = self.connection.cursor()

        self.scan_connection = pymysql.connect(
            host=config.scan_db_host,
            user=config.scan_db_user,
            password=config.scan_db_password,
            database=config.db_name_scan,
            port=config.scan_db_port,
            autocommit=True
        )
        self.scan_cursor = self.scan_connection.cursor()

        self.portal = config.db_name_portal
        self.schema = config.scan_type
        self.ingress = config.db_name_portal
        self.sendwebhook_url = config.whsend_url

        if config.scan_type.lower() == "mad":
            self.queries = QUERIES["mad"]
        else:
            self.queries = QUERIES["rdm"]

    def update_point(self, wp_type, name, url, wp_id):
        name = str(name).replace("'", "\\'")
        if wp_type == "Stop":
            self.scan_cursor.execute(self.queries["update_stop"], (name, url, wp_id))
        elif wp_type == "Gym":
            self.scan_cursor.execute(self.queries["update_gym"], (name, url, wp_id))

    def update_portal(self, data):
        query = (
            "INSERT INTO ingress_portals (external_id, name, url, lat, lon, updated, imported) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE updated=VALUES(updated), name=VALUES(name), url=VALUES(url), "
            "lat=VALUES(lat), lon=VALUES(lon)"
        )

        self.cursor.executemany(query, data)
        self.connection.commit()  # Commit the changes
        result = self.cursor.rowcount  # Get the number of affected rows
        return result

    def send_webhook(self, data):
        webhook_url = self.sendwebhook_url  # Use the URL from the config
        headers = {'Content-Type': 'application/json'}

        # Prepare the payload for the webhook
        payload = {
            'data': data,
            'message': 'New data to be sent'
        }

        # Send the payload as JSON via POST request
        response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

        if response.status_code == 200:
            print("Webhook sent successfully")
        else:
            print("Failed to send webhook")


    def get_empty_gyms(self):
        self.scan_cursor.execute(self.queries["empty_gyms"])
        gyms = self.scan_cursor.fetchall()
        return gyms


    def get_empty_stops(self):
        self.scan_cursor.execute(self.queries["empty_stops"])
        stops = self.scan_cursor.fetchall()
        return stops


    def close(self):
        self.cursor.close()
        self.connection.close()
