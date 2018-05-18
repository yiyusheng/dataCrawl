import requests
proxy = {'https':'socks5h://127.0.0.1:1080'}
url = "https://bitmex.com/api/v1/chat?count=500&reverse=true&channelID=2"
resp = requests.get(url=url,proxies=proxy)
print(resp.text)
