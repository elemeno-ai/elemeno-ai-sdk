import pytest
from omegaconf import OmegaConf
from feast import RepoConfig
from elemeno_ai_sdk.ml.features.config.repo_config import create_repo_config

def test_create_repo_config_from_yaml_valid_config():
    # Define a valid configuration
    valid_config = OmegaConf.create({
        'feature_store': {
            'sink': {
                'type': 'redshift',
                'params': {
                    'region': 'us-west-2',
                    'cluster_id': 'test-cluster',
                    'database': 'test-db',
                    'user': 'test-user',
                    's3_staging_location': 's3://test-bucket',
                    'iam_role': 'test-role'
                }
            },
            'registry': '/path/to/registry',
            'provider': 'aws',
            'project': 'testproject',
            'online_store': {
                'type': 'dynamodb',
                'params': {
                    'region': 'us-west-2',
                    'batch_size': 100,
                    'consistent_reads': True,
                    'endpoint_url': 'http://localhost:8000',
                    'table_name_template': 'test-template'
                }
            }
        }
    })

    # Call the function
    repo_config = create_repo_config(valid_config)

    # Check the result
    assert isinstance(repo_config, RepoConfig)
    assert repo_config.registry.path == '/path/to/registry'
    assert repo_config.offline_store.type == 'redshift'
    assert repo_config.online_store.type == 'dynamodb'

def test_create_repo_config_from_yaml_invalid_offline_store_type():
    # Define an invalid configuration
    invalid_config = OmegaConf.create({
        'feature_store': {
            'sink': {
                'type': 'invalid-type',
                'params': {}
            },
            'registry': '/path/to/registry',
            'provider': 'aws',
            'project': 'testproject',
            'online_store': {
                'type': 'dynamodb',
                'params': {}
            }
        }
    })

    # Call the function and check the exception
    with pytest.raises(Exception, match="Unsupported offline store type: invalid-type"):
        create_repo_config(invalid_config)

def test_create_repo_config_from_yaml_invalid_online_store_type():
    # Define an invalid configuration
    invalid_config = OmegaConf.create({
        'feature_store': {
            'sink': {
                'type': 'redshift',
                'params': {}
            },
            'registry': '/path/to/registry',
            'provider': 'aws',
            'project': 'testproject',
            'online_store': {
                'type': 'invalid-type',
                'params': {}
            }
        }
    })

    # Call the function and check the exception
    with pytest.raises(Exception, match="Unsupported online store type: invalid-type"):
        create_repo_config(invalid_config)
