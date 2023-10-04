from datetime import datetime
from dateutil.parser import parse
from typing import Dict


class Deploy:
    __id: str
    __project_id: str
    __model_name: str
    __name: str
    __account: str
    __service_url: str
    __checkpoint_path: str
    __run_id: str
    __created_at: datetime
    __updated_at: datetime

    def __init__(self, deploy: Dict) -> None:
        self.__id = deploy["id"]
        self.__project_id = deploy["projectId"]
        self.__model_name = deploy["modelId"]
        self.__name = deploy["name"]
        self.__account = deploy["account"]
        self.__service_url = deploy["serviceUrl"]
        self.__checkpoint_path = deploy["checkpointPath"]
        self.__run_id = deploy["runID"]
        self.__created_at = parse(deploy["createdAt"])
        self.__updated_at = parse(deploy["updatedAt"])

    @property
    def id(self) -> str:
        return self.__id

    @property
    def project_id(self) -> str:
        return self.__project_id

    @property
    def model_name(self) -> str:
        return self.__model_name

    @property
    def name(self) -> str:
        return self.__name

    @property
    def account(self) -> str:
        return self.__account

    @property
    def service_url(self) -> str:
        return self.__service_url

    @property
    def checkpoint_path(self) -> str:
        return self.__checkpoint_path

    @property
    def run_id(self) -> str:
        return self.__run_id

    @property
    def created_at(self) -> datetime:
        return self.__created_at

    @property
    def updated_at(self) -> datetime:
        return self.__updated_at

    @id.setter
    def id(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: id.")

    @project_id.setter
    def project_id(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: project_id.")

    @model_name.setter
    def model_name(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: model_name.")

    @name.setter
    def name(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: name.")

    @account.setter
    def account(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: account.")

    @service_url.setter
    def service_url(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: service_url.")

    @checkpoint_path.setter
    def checkpoint_path(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: checkpoint_path.")

    @run_id.setter
    def run_id(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: run_id.")

    @created_at.setter
    def created_at(self, value: datetime) -> None:
        raise AttributeError("Cannot set readonly property: created_at.")

    @updated_at.setter
    def updated_at(self, value: datetime) -> None:
        raise AttributeError("Cannot set readonly property: updated_at.")
