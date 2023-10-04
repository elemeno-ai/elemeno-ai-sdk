from elemeno_ai_sdk.ml.genai.entities.models import Model
from elemeno_ai_sdk.ml.genai.protocols.finetune_interface import IFineTune


class Dreambooth(IFineTune):
    model: Model = None
    job_name: str = None
    class_word: str = None
    images_path: str = None
    reg_images_path: str = None

    def __init__(
        self,
        model: Model,
        job_name: str,
        class_word: str,
        images_path: str,
        reg_images_path: str,
    ):
        self.model = model
        self.job_name = job_name
        self.class_word = class_word
        self.images_path = images_path
        self.reg_images_path = reg_images_path

    def get_body(self):
        return {
            "model_cos_file": self.model.ptweights[0].path.replace(
                "s3://elemeno-cos/", ""
            ),
            "finetuneMethod": "dreambooth",
            "job_name": self.job_name,
            "class_word": self.class_word,
        }

    def get_file(self):
        return {
            "images": ("images", open(self.images_path, "rb")),
            "reg_images": (
                "reg_images_path",
                open(self.reg_images_path, "rb"),
            ),
        }

    def get_endpoint(self):
        return "fine-tune/dreambooth"
