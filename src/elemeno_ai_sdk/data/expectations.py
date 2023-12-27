import typing

import requests

from src.elemeno_ai_sdk.ml.mlhub_client import PROD_URL, DEV_URL

class RuleSet:
    route = "/suite"
    def __init__(self, base_url, auth_header):
        self.base_url = base_url
        self.auth_header = auth_header

    def add(self, name: str, rule_set: dict):
        body = {"name": name, "expectations": rule_set}
        return requests.post(
            url=self.base_url + self.route,
            json=body,
            headers=self.auth_header,
        )

    def update(self, name: str, rule_set: dict):
        body = {"name": name, "expectations": rule_set}
        return requests.put(
            url=self.base_url + self.route,
            json=body,
            headers=self.auth_header,
        )

    def delete(self, name: str):
        return requests.put(
            url=self.base_url + self.route,
            json={"name": name},
            headers=self.auth_header,
        )

    def get(self):
        return requests.get(
            url=self.base_url + self.route,
            headers=self.auth_header,
        )

    def describe(self, name: str):
        return requests.get(
            url=self.base_url + self.route + "/" + name,
            headers=self.auth_header,
        )

    def list_available_expectations_rules(self):
        return requests.get(
            url=self.base_url + "/expectations",
            headers=self.auth_header,
        )


class DataExpectationsClient:
    def __init__(self, api_key: str, env: str = "prod"):
        self.url = DEV_URL if env == "dev" else PROD_URL
        self.rule_sets = RuleSet(
            base_url=self.url, 
            auth_header={"x-api-key": api_key},
        )

    def validate(self, rule_set_name: str, data_connector_name: str):
        raise NotImplementedError
