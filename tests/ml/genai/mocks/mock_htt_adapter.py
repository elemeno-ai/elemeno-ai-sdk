class MockHttpAdapter:
    url: [str] = None
    headers: [str] = None
    body: any = None
    file: str = None

    def __init__(self) -> None:
        self.url = []
        self.headers = []
        self.body = None

    def get(self, url: str, headers: str) -> str:
        self.url.append(url)
        self.headers.append(headers)
        return headers

    def post(self, url: str, headers: str, body, file=None) -> None:
        self.url.append(url)
        self.headers.append(headers)
        self.body = body
        if file is not None:
            self.file = file
        return headers

    def delete(self, url, headers):
        self.url.append(url)
        self.headers.append(headers)
        return headers
