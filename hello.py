import json
import time

import requests


while True:
    try:
        response = requests.get(
            "http://localhost:6112/events?type=peon&peon_id=abc&queues=['DEFAULT']",
            stream=True,
        )
        for line in response.iter_content(chunk_size=None):
            if line:
                print(line)
                message = line.decode().split("data:")[1]
                message = json.loads(message)
                print(message)
    except requests.exceptions.ConnectionError as e:
        print("failed to connect, likely server offline", e)
        time.sleep(1)
    except Exception as e:
        print("error", e)
        time.sleep(1)
