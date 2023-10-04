from datetime import datetime
from dateutil.parser import parse
from typing import Dict


class FineTune:
    __id: str
    __projectId: str
    __runID: str
    __artifactURI: str
    __methodName: str
    __status: str
    __experimentId: str
    __createdAt: datetime

    def __init__(self, finetune: Dict) -> None:
        self.__id = finetune["id"]
        self.__projectId = finetune["projectId"]
        self.__runID = finetune["runId"]
        self.__artifactURI = finetune["artifactUri"]
        self.__methodName = finetune["methodName"]
        self.__status = finetune["status"]
        self.__experimentId = finetune["experimentId"]
        self.__createdAt = parse(finetune["createdAt"])

    @property
    def id(self) -> str:
        return self.__id

    @property
    def projectId(self) -> str:
        return self.__projectId

    @property
    def runId(self) -> str:
        return self.__runID

    @property
    def artifactURI(self) -> str:
        return self.__artifactURI

    @property
    def methodName(self) -> str:
        return self.__methodName

    @property
    def status(self) -> str:
        return self.__status

    @property
    def experimentId(self) -> str:
        return self.__experimentId

    @property
    def createdAt(self) -> datetime:
        return self.__createdAt

    @id.setter
    def id(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: id.")

    @projectId.setter
    def projectId(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: projectId.")

    @runId.setter
    def runId(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: runId.")

    @artifactURI.setter
    def artifactURI(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: artifactURI.")

    @methodName.setter
    def methodName(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: methodName.")

    @status.setter
    def status(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: status.")

    @experimentId.setter
    def experimentId(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: experimentId.")

    @createdAt.setter
    def createdAt(self, value: datetime) -> None:
        raise AttributeError("Cannot set readonly property: createdAt.")

    def __str__(self) -> str:
        return f"FineTune: {self.__id} Project_id: {self.__projectId} Status: {self.__status}"
