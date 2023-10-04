# GenAIClient

GenAIClient is a Python package that provides methods for interacting with GenAI HUB.

# Development

This class exposes methods to control features on the GenAI HUB WebSite.

```python
from elemeno_ai_sdk.ml.genai.genai_client import GenAIClient
genai_client = GenAIClient()
```
If not **env** parameter passed, it will access the DEV environment. To use PRD, pass **env** as "PRD"

It is necessary to set the environment variable API_KEY for the package to function correctly.

**Windows - PowerShell**
```sh
$env:API_KEY="your api_key"
```
**Linux**
```sh
export API_KEY="your api_key"
```

## Models
Responsible for listing the available types of models.
```python
#return object models
genai_client.models

#printing list models
print(genai_client.models)

#selecting Model
genai_client.models.get('Llama V2')

#priting info the model
print(genai_client.models.get('Llama V2'))
```

## Project
Responsible for listing projects created, create new project, delete project or find projects.
```python
#return object project
genai_client.projects

#printing list projects
print(genai_client.projects)

#Create project
project_name = "newProject"
model_name = "Llama V2"
genai_client.projects.create(project_name, model_name)

#Delete project
genai_client.projects.delete(project_name)

#Find all projects
genai_client.projects.find_all()

#Find projects that are equal to or contain the name
genai_client.projects.find_by_name(project_name)

#Find projects that are equal to or contain the model name
genai_client.projects.find_by_model("model_name")

#Find projects the created between date_start and date_end
date_start = date.today() - timedelta(5)
date_end = date.today()
genai_client.projects.find_by_created(date_start, date_end)

#Find projects the last used between date_start and date_end
date_start = date.today() - timedelta(5)
date_end = date.today()
genai_client.projects.find_by_last_used(date_start, date_end)
```

## Deploy
Responsible for listing deploy created, create new deploy or find deploys.
```python
#return object Deploy
genai_client.deploys

#printing list deploys
print(genai_client.deploys)

#Create deploy
version = 1
project_id = genai_client.projects.find_by_name(project_name)[0].id
deploy_name = f"{project_name}_{version}"

genai_client.deploys.deploy(project_id, deploy_name, model_name)

#Find deploys that are equal to or contain the name
genai_client.deploys.find_by_name(deploy_name)

#Find deploys that are equal to or contain the project_id
genai_client.deploys.find_by_project(project_id)
```


## FineTune
Responsible for listing finetunes created, fit finetune or find finetune.
```python
# FineTune using Lora
from elemeno_ai_sdk.ml.genai.finetunes.lora import Lora
lora =  Lora(genai_client.models.get('Llama V2'), "C:\\temp", "teste.txt")
genai_client.finetune.fit(genai_client.projects.find_by_name("Project")[0].id, lora)

# FineTune using Dreambooth
from elemeno_ai_sdk.ml.genai.finetunes.dreambooth import Dreambooth
dreambooth =  Dreambooth(genai_client.models.get("Llama V2"), "Your JobName", "Your Class Word", "Images Path", "Reg Images Path")
genai_client.finetune.fit(genai_client.projects.find_by_name("Project")[0].id, dreambooth)
```

## PlayGround
Testing your deployment
```python
playground = genai_client.playground
deploy_id = genai_client.deploys.find_by_name("MyDeploy")[0].id
prompt = "Your question or command use to Generative AI"
playground.prompt(deploy_id, prompt)
```
