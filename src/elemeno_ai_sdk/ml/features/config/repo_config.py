from typing import Union

from feast import RepoConfig
from omegaconf import DictConfig, ListConfig


online_feast_params = {
    "dynamodb": [
        "region",
        "batch_size",
        "consistent_reads",
        "endpoint_url",
        "table_name_template",
    ]
}

offline_feast_params = {
    "redshift": [
        "region",
        "cluster_id",
        "database",
        "user",
        "s3_staging_location",
        "iam_role",
        "workgroup",
    ],
    "bigquery": [
        "project_id",
        "billing_project_id",
        "dataset",
        "gcs_staging_location",
        "location",
    ],
}


def create_params(config_object, expected_params):
    params = {}
    for param in expected_params:
        if param not in params:
            params[param] = config_object.get(param)
    return params


def create_repo_config(configs: Union[DictConfig, ListConfig]) -> RepoConfig:
    # Extract necessary fields
    feature_store_config = configs.get("feature_store", {})
    sink_config = feature_store_config.get("sink", {})
    offline_store_type = sink_config.get("type")

    if offline_store_type not in offline_feast_params:
        raise Exception("Unsupported offline store type: {}".format(offline_store_type))
    expected_params = offline_feast_params[offline_store_type]
    offline_store_config = create_params(sink_config.get("params"), expected_params)
    offline_store_config["type"] = offline_store_type

    registry = feature_store_config.get("registry")
    provider = feature_store_config.get("provider")
    project = feature_store_config.get("project")

    online_store = feature_store_config.get("online_store", {})
    online_store_type = online_store.get("type")

    if online_store_type not in online_feast_params:
        raise Exception("Unsupported online store type: {}".format(online_store_type))
    expected_params = online_feast_params[online_store_type]
    online_store_config = create_params(online_store.get("params"), expected_params)
    online_store_config["type"] = online_store_type

    try:
        # Create RepoConfig
        repo_config = RepoConfig(
            registry=registry,
            offline_store=offline_store_config,
            online_store=online_store_config,
            provider=provider,
            project=project,
            # Add other necessary fields here
        )

        return repo_config
    except Exception as e:
        raise Exception(
            "Failed to create config, there is an error in the feature_store section of your elemeno.yaml. More details: {}".format(
                e
            )
        )
