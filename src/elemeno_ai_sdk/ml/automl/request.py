from dataclasses import asdict, dataclass


@dataclass
class AutoMLRequest:
    featureTableName: str
    featureSelected: str = ""
    idColumn: str
    targetName: str
    startDate: str = ""
    endDate: str = ""
    task: str
    scoring: str
    numFeatures: int
    generations: int

    def to_dict(self):
        return asdict(self)
