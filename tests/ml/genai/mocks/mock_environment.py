class MockEnvironment:
    headers = "headers"
    url: [str] = None
    raise_exception = False
    msg_exception = None

    def __init__(self):
        self.url = []
        self.raise_exception = False
        self.msg_exception = None

    def make_url(self, value: str) -> str:
        if self.raise_exception:
            raise ValueError(self.msg_exception)

        self.url.append(value)
        return value
