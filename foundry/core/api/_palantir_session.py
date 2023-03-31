import requests


class PalantirSession(requests.Session):
    """Wrapper class to persist hostname and credentials across http requests.

    :param hostname: String for the hostname of the desired endpoints
    """

    def __init__(self, hostname: str, preview: bool = False) -> None:
        super().__init__()
        self._set_hostname(hostname)
        self.preview = preview

    def _set_hostname(self, hostname: str) -> None:
        self.hostname = hostname.removeprefix("https://").removeprefix("http://")
