import requests



def get_request(url):
    # passing parms to get request

    parms = {
        "per_page": 75,
    }
    response = requests.get(url, params=parms)
    data = response.json()
    return data


if __name__ == "__main__":
    url = "https://api.openbrewerydb.org/v1/breweries"
    print(get_request(url))
    print(type(get_request(url)))
    print(len(get_request(url)))