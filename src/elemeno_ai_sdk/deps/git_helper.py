import os
from git import Repo
from git import Git
import logging

class GitHelper:
    
    def __init__(self, ssh_key_path: str):
        self._ssh_key_path = ssh_key_path
    
    def ssh_clone(self, repository_url: str, branch: str = "master"):
        git_ssh_identity_file = os.path.expanduser(self._ssh_key_path)
        git_ssh_cmd = 'ssh -i %s' % git_ssh_identity_file
        with Git().custom_environment(GIT_SSH_COMMAND=git_ssh_cmd):
            project_name = repository_url.split("/")[-1][:-4]
            Repo.clone_from(repository_url, project_name, branch=branch, env=dict(GIT_SSH_COMMAND=git_ssh_cmd))
            logging.info("Finished cloning project")