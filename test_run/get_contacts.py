import requests

api_url = "https://api.hubapi.com/contacts/v1/lists/30/contacts/all"

headers = {
    'Authorization': 'Bearer pat-na1-111e57ab-b515-4a74-9c35-d7a64c10aa0e'
}

response = requests.get(api_url, headers=headers)

if response.status_code == 200:
    data = response.json()

    for x in data["contacts"]:
        print(f'Name: {x["properties"]["firstname"]["value"]} {x["properties"]["lastname"]["value"]}\
        Email: {x["identity-profiles"][0]["identities"][0]["value"]}')
    # print(data)
else:
    print(f"Request failed with status code: {response.status_code}")
