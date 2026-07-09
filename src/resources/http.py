from urllib.request import Request, urlopen

from dagster import ConfigurableResource


class RetsinformationHttpResource(ConfigurableResource):
    timeout_seconds: float = 30.0
    user_agent: str = "opensourcelaw-retsinformation-ingest/0.1"

    def get_bytes(self, url: str) -> bytes:
        request = Request(url, headers={"User-Agent": self.user_agent})

        with urlopen(request, timeout=self.timeout_seconds) as response:
            return response.read()
