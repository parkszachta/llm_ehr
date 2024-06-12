# https://llama.meta.com/docs/llama-everywhere/running-meta-llama-on-mac/

import requests

url = "http://localhost:11434/api/chat"

def llama3(prompt):
    data = {
        "model": "llama3",
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "stream": False
    }
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, json=data)
    return(response.json()['message']['content'])

num1 = int(llama3("Pick a number from 1 to 10. Say only the number and nothing else."))
print(num1)
num2 = int(llama3("Pick a number from 1 to 10, besides 7. Say only the number and nothing else."))
print(num2)
print(num1 * num2)