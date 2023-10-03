from typing import Any, Literal, Optional, Union
import requests

PROD_URL = "https://c3po.ml.semantixhub.com"
DEV_URL = "https://c3po-stg.ml.semantixhub.com"


class GenAIClient:
    def __init__(self, env: Literal["prod", "dev"], api_key: str) -> None:
        self.env = env
        self.api_key = api_key


    @property
    def base_url(self):
        base_url = None
        if self.env == "prod":
            base_url = PROD_URL
        elif self.env == "dev":
            base_url = DEV_URL
        else:
            raise ValueError("Invalid environment. Please use dev or prod.")
        return base_url


    def _make_url(self, endpoint: str) -> str:
        return f"{self.base_url}/generative/{endpoint}"    

    @property
    def headers(self):
        #return {"Content-Type": "application/json", "x-api-key": self.api_key}
        return {"x-api-key": self.api_key}

    def _send_request(
            self, 
            method:str,             
            url:str, 
            headers:Optional[dict]=None,
            json_data:Optional[dict]=None,
            data:Optional[dict]=None, 
            files:Optional[dict]=None,            
            output_fmt:Literal['json', 'text']='json') -> Union[dict, str]:
        """
        Send an HTTP request to the ML Semantix Hub API.

        Args:
            method (str): The HTTP method (get, post, DELETE).
            url (str): The API url to send the request to.
            data (dict): The JSON data to include in the request body (for post requests).
            expected_status (int): The expected HTTP status code for a successful response.
            output (str): The desired response format ('json' or 'text').

        Returns:
            dict or str: The API response data.

        Example:
            >>> response = self._send_request('get', 'supported-model')
        """

      
        kwargs = {}
        kwargs['method'] = method
        kwargs['url'] = url
        kwargs['headers'] = headers or self.headers
        if json_data:
            kwargs['json'] = json_data
        if data:
            kwargs['data'] = data
        if files:
            kwargs['files'] = files
        
        response = requests.request(**kwargs)           

        response.raise_for_status()
        
        if output_fmt == 'json':
            return response.json()
        else:
            return response.text
                 

    def list_supported_models(self):
        """
        List supported models.

        Returns:
            list: A list of supported models.

        Example:
            >>> supported_models = genai.list_supported_models()
        """
        url = self._make_url('supported-model')
        return self._send_request('get', url)


    def list_user_projects(self):
        """
        List user projects.

        Returns:
            list: A list of user projects.

        Example:
            >>> user_projects = genai.list_user_projects()
        """
        url = self._make_url('project')
        return self._send_request('get', url)


    def create_user_project(self, model_id:str, project_name:str):
        """
        Create a user project.

        Args:
            model_id (str): The ID of the model to use.
            project_name (str): The name of the user project.

        Returns:
            dict: The created user project.

        Example:
            >>> project = genai.create_user_project('llama', 'my-project')
        """
        url = self._make_url('project')
        data = {
            'modelId': model_id,
            'name': project_name
        }
        return self._send_request('post', url, json_data=data)


    def delete_project(self, project_id:str):
        """
        Delete a user project.

        Args:
            project_id (str): The ID of the project to delete.

        Returns:
            dict: The result of the project deletion.

        Example:
            >>> result = genai.delete_project('64fb51a3e18de22bbbaa3b7d')
        """
        url = self._make_url(f'project/{project_id}')
        return self._send_request('delete', url)

    def list_project_deployments(self, project_id:str):
        """
        List deploys for a user project.

        Args:
            project_id (str): The ID of the project to list deploys for.

        Returns:
            list: A list of project deploys.

        Example:
            >>> deploys = genai.list_project_deployments('64fb2cd2e18de22bbbaa3b64')
        """
        url = self._make_url(f'project/{project_id}/model-deploy?offset=0')
        return self._send_request('get', url)

    def deploy(self, project_id:str, deploy_name:str):
        """
        Create a project deploy.

        Args:
            project_id (str): The ID of the project to deploy to.
            deploy_name (str): The name of the deploy.
            checkpoint_path (str): The path to the checkpoint for the deploy.

        Returns:
            dict: The created project deploy.

        Example:
            >>> deploy = genai.create_project_deploy(
            ...     '64fb2cd2e18de22bbbaa3b64',
            ...     'terceiro_deploy',
            ...     's3://elemeno-cos/models/genai/llama-v2/llama-2-7b-chat-hf/models_hf/7B/pytorch_model-00001-of-00002.bin'
            ... )
        """
        url = self._make_url(f'project/{project_id}/model-deploy')
        data = {
            "name": deploy_name,
            "checkpointPath": ""#checkpoint_path
        }
        return self._send_request('post', url, json_data=data)


    def list_project_fine_tunes(self, project_id:str):
        """
        List fine-tunes for a user project.

        Args:
            project_id (str): The ID of the project to list fine-tunes for.

        Returns:
            list: A list of project fine-tunes.

        Example:
            >>> fine_tunes = genai.list_project_fine_tunes('64fb2cd2e18de22bbbaa3b64')
        """
        url = self._make_url(f'project/{project_id}/fine-tune')
        return self._send_request('get', url)
    

    def fine_tune_lora(
        self,
        project_id: str,
        dataset_path: str,
        model_cos_file :str, 
        dataset_name: str = 'data.json',        
        batch_size: int = 128,
        micro_batch_size: int = 4,
        num_epochs: int = 3,
        learning_rate: int = 0.0001,
        cutoff_len: int = 512,
        lora_r: int = 8,
        lora_alpha: int = 16,
        lora_dropout: int = 0.05
        ):
        """
        The `fine_tune_lora` function is used to fine-tune a LoRA model with specified parameters using a given dataset and a pre-trained model.
        
        Args:
          project_id (str): The ID of the project you want to fine-tune the model for.
          dataset_path (str): The path to the dataset file that will be used for fine-tuning the model.
          model_cos_file (str): The `model_cos_file` parameter is the path to the model file stored in the IBM Cloud Object Storage (COS). It should be a string representing the location of the model file in the COS.
          dataset_name (str): The name of the dataset file that will be used for fine-tuning the model. Defaults to data.json
          batch_size (int): The batch size is the number of samples that will be propagated through the network at once. It determines how many samples are processed before the model's parameters are updated. Defaults to 128
          micro_batch_size (int): The `micro_batch_size` parameter is used to specify the number of samples in each micro-batch during training. It determines how many samples are processed before updating the model's weights. Defaults to 4
          num_epochs (int): The `num_epochs` parameter specifies the number of times the model will iterate over the entire dataset during training. Each iteration is called an epoch. Defaults to 3
          learning_rate (int): The learning rate is a hyperparameter that determines the step size at each iteration while updating the model's parameters during training. It controls how much the model's parameters are adjusted in response to the estimated error. A smaller learning rate will result in slower convergence but potentially more accurate results, while a larger learning
          cutoff_len (int): The `cutoff_len` parameter is used to specify the maximum length of the input sequences during fine-tuning. Any input sequence longer than this length will be truncated. Defaults to 512
          lora_r (int): The parameter `lora_r` in the `fine_tune_lora` function is used to specify the number of attention heads in the LoRA (Localized Relevance Attention) mechanism. It determines how the input sequence is divided into multiple parts for attention computation. Increasing the value of `l. Defaults to 8
          lora_alpha (int): The `lora_alpha` parameter is a hyperparameter used in the LoRA (Localized Randomized Attention) method for fine-tuning a language model. It controls the strength of the attention mechanism during fine-tuning. A higher value of `lora_alpha` increases the importance of the localized. Defaults to 16
          lora_dropout (int): The `lora_dropout` parameter is used to specify the dropout rate for the LoRA (Localized Randomization) layer during fine-tuning. Dropout is a regularization technique that randomly sets a fraction of input units to 0 at each update during training, which helps prevent overfitting. The
        
        Returns:
          the result ofthe response from the API call made to the specified URL.
        """

        url = self._make_url(f'project/{project_id}/fine-tune/lora')
                               
        data = {
            'model_cos_file': model_cos_file.replace('s3://elemeno-cos/', ''),
            'finetuneMethod': 'lora',          
            'batch_size': batch_size,
            'micro_batch_size': micro_batch_size,
            'num_epochs': num_epochs,
            'learning_rate': learning_rate,
            'cutoff_len': cutoff_len,
            'lora_r': lora_r,
            'lora_alpha': lora_alpha,
            'lora_dropout': lora_dropout,
        }

        file = {
            'dataset': (dataset_name, open(dataset_path, 'rb')),
        }
       
        return self._send_request(
            'post',
            url, 
            headers=self.headers, 
            data=data, 
            files=file
        )


    def fine_tune_dreambooth(
            self,
            project_id: str,
            model_cos_file:str,
            job_name:str,
            class_word:str,
            images_path:str,
            reg_images_path:str
        ):

        url = self._make_url(f'project/{project_id}/fine-tune/dreambooth')
    
        data = {
            'model_cos_file': model_cos_file.replace('s3://elemeno-cos/', ''),
            'finetuneMethod': 'dreambooth',
            'job_name': job_name,
            'class_word': class_word           
        }
    
        files = {
            'images': ('images', open(images_path, 'rb')),
            'reg_images': ('reg_images_path', open(reg_images_path, 'rb')),
        }

        return self._send_request(
            'post',
            url, 
            headers=self.headers, 
            data=data, 
            files=files
        )
    

    def model_playground(self, deploy_id:str, prompt:str) -> str:
        """
        The `model_playground` function sends a POST request to a specified URL with a prompt and returns the response as text.
        
        Args:
          deploy_id (str): The `deploy_id` parameter is a string that represents the deployment ID of the model you want to use for inference. This ID is used to construct the URL for making the API request.
          prompt (str): The `prompt` parameter is a string that represents the input prompt for the model. It is the text that you want to provide as input to the model for generating a response or making predictions.
        
        Returns:
          the result of the `_send_request` method, which is a string.
        """
        url = f'https://infserv-{deploy_id}.app.elemeno.ai/v0/inference'
        headers = {
            'Authorization': f'Bearer {self.api_key}'
        }  
        data = {'prompt': [prompt]}

        return self._send_request(
            'post', 
            url,
            headers=headers, 
            json_data=data, 
            output_fmt='text'
        )
    
   