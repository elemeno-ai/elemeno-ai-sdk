from datetime import datetime
from dateutil.parser import parse
from typing import Dict


class Project:
    __id: str
    __name: str
    __model_name: str
    __created_at: datetime
    __last_used: datetime

    def __init__(self, project: Dict) -> None:
        self.__id = project["id"]
        self.__name = project["name"]
        self.__model_name = project["modelId"]
        self.__created_at = parse(project["createdAt"])
        self.__last_used = parse(project["lastUsed"])

    @property
    def id(self) -> str:
        return self.__id

    @property
    def name(self) -> str:
        return self.__name

    @property
    def model_name(self) -> str:
        return self.__model_name

    @property
    def created_at(self) -> datetime:
        return self.__created_at

    @property
    def last_used(self) -> datetime:
        return self.__last_used

    @id.setter
    def id(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: id.")

    @name.setter
    def name(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: name.")

    @model_name.setter
    def model_name(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: model_name.")

    @created_at.setter
    def created_at(self, value: datetime) -> None:
        raise AttributeError("Cannot set readonly property: created_at.")

    @last_used.setter
    def last_used(self, value: datetime) -> None:
        raise AttributeError("Cannot set readonly property: last_used.")

    def __str__(self) -> str:
        return f"Project: {self.__name}"
