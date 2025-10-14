import requests

def paginatedGet(url, headers, inputdata):
    perPageData = {"per_page": 100}
    mergedData = {**inputdata, **perPageData}
    response = requests.get(url, data=mergedData, headers=headers)
    data = response.json()
    if 'next' in response.links:
        data = data + paginatedGet(response.links['next']['url'], headers, inputdata)
 
    return data