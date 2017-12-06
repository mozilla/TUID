import requests
import json
class Web:
    def get(url):
        response = requests.get(url)
        return json.loads(response.text)

    def get_string(url):
        return requests.get(url)