from elemeno_ai_sdk.ml.genai.entities.models import Model
from elemeno_ai_sdk.ml.genai.protocols.finetune_interface import IFineTune


class Lora(IFineTune):
    model: Model = None
    dataset_path: str = None
    dataset_file: str = None
    batch_size: int = None
    micro_batch_size: int = None
    num_epochs: int = None
    learning_rate: float = None
    cutoff_len: int = None
    lora_r: int = None
    lora_alpha: int = None
    lora_dropout: float = None

    def __init__(
        self,
        model: Model,
        dataset_path: str,
        dataset_file: str,
        batch_size: int = 128,
        micro_batch_size: int = 4,
        num_epochs: int = 3,
        learning_rate: float = 0.0001,
        cutoff_len: int = 512,
        lora_r: int = 8,
        lora_alpha: int = 16,
        lora_dropout: float = 0.05,
    ) -> None:
        self.model = model
        self.dataset_path = dataset_path
        self.dataset_file = dataset_file
        self.batch_size = batch_size
        self.micro_batch_size = micro_batch_size
        self.num_epochs = num_epochs
        self.learning_rate = learning_rate
        self.cutoff_len = cutoff_len
        self.lora_r = lora_r
        self.lora_alpha = lora_alpha
        self.lora_dropout = lora_dropout

    def get_body(self) -> dict:
        return {
            "model_cos_file": self.model.ptweights[0].path.replace(
                "s3://elemeno-cos/", ""
            ),
            "batch_size": self.batch_size,
            "micro_batch_size": self.micro_batch_size,
            "num_epochs": self.num_epochs,
            "learning_rate": self.learning_rate,
            "cutoff_len": self.cutoff_len,
            "lora_r": self.lora_r,
            "lora_alpha": self.lora_alpha,
            "lora_dropout": self.lora_dropout,
        }

    def get_file(self) -> dict:
        return {"dataset": (self.dataset_file, open(self.dataset_path, "rb"))}

    def get_endpoint(self):
        return "fine-tune/lora"
