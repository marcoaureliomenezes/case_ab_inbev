import requests

class BreweriesCrawler:

    def __init__(self, logger):
        self.logger = logger


    def get_page(self, page_size=50, page=1):
        base_url = "https://api.openbrewerydb.org/v1/breweries"
        response = requests.get(base_url, params={"per_page": page_size, "page": page})
        data = response.json()
        return data
    
    def get_all_pages(self, limit=None):
        page = 1
        while True:
            payload = self.get_page(page=page)
            if len(payload) == 0: break
            yield page, payload
            if limit and page >= limit: break
            page += 1

    

    def get_metadata(self):
        base_url = "https://api.openbrewerydb.org/v1/breweries/meta"
        response = requests.get(base_url)
        data = response.json()
        return data