from typing import Dict


class PtWeight:
    __name: str
    __path: str
    __supportedFineTuneMethods: [str]

    def __init__(self, ptWeight: Dict) -> None:
        self.__name = ptWeight["name"]
        self.__path = ptWeight["path"]
        self.__supportedFineTuneMethods = ptWeight["supportedFineTuneMethods"]

    @property
    def name(self) -> str:
        return self.__name

    @property
    def path(self) -> str:
        return self.__path

    @property
    def supportedFineTuneMethods(self) -> [str]:
        return self.__supportedFineTuneMethods

    @name.setter
    def name(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: name.")

    @path.setter
    def path(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: path.")

    @supportedFineTuneMethods.setter
    def supportedFineTuneMethods(self, value: [str]) -> None:
        raise AttributeError(
            "Cannot set readonly property: supportedFineTuneMethods."
        )

    def __str__(self) -> str:
        return str(
            "{name: "
            + self.name
            + " path: "
            + self.path
            + " supportedFineTuneMethods: "
            + str(self.supportedFineTuneMethods)
            + "}"
        )


class License:
    __type: str
    __link: str

    def __init__(self, license: Dict) -> None:
        self.__type = license["type"]
        self.__link = license["link"]

    @property
    def type(self) -> str:
        return self.__type

    @property
    def link(self) -> str:
        return self.__link

    @type.setter
    def type(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: type.")

    @link.setter
    def link(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: link.")

    def __str__(self) -> str:
        return str("{type: " + self.type + "\n" + " link: " + self.link + "}")


class Model:
    __id: str
    __github_uri: str
    __model_name: str
    __ptweights: [PtWeight]
    __license: License

    def __init__(self, models: Dict) -> None:
        self.__id = models["id"]
        self.__github_uri = models["githubURI"]
        self.__model_name = models["modelName"]
        self.__ptweights = [PtWeight(x) for x in models["ptWeights"]]
        self.__license = License(models["license"])

    @property
    def id(self) -> str:
        return self.__id

    @property
    def github_uri(self) -> str:
        return self.__github_uri

    @property
    def model_name(self) -> str:
        return self.__model_name

    @property
    def ptweights(self) -> [PtWeight]:
        return self.__ptweights

    @property
    def license(self) -> License:
        return self.__license

    @id.setter
    def id(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: id.")

    @github_uri.setter
    def github_uri(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: github_uri.")

    @model_name.setter
    def model_name(self, value: str) -> None:
        raise AttributeError("Cannot set readonly property: model_name.")

    @ptweights.setter
    def ptweights(self, value: [PtWeight]) -> None:
        raise AttributeError("Cannot set readonly property: ptweights.")

    @license.setter
    def license(self, value: License) -> None:
        raise AttributeError("Cannot set readonly property: license.")

    def __str__(self) -> str:
        return str(
            "id: "
            + self.id
            + "\n"
            + " github_uri: "
            + self.github_uri
            + "\n"
            + " model_name: "
            + self.model_name
            + "\n"
            + " ptweights: "
            + str([str(x) for x in self.ptweights])
            + "\n"
            + " license: "
            + str(self.license)
        )
