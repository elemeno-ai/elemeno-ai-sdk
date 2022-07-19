from elemeno_ai_sdk.ml.features.base_feature_store import BaseFeatureStore

class Pipe:

  @staticmethod
  def To(fs: BaseFeatureStore, fn, *args, **kwargs):
    return fn(fs._memory, *args, **kwargs)


def create_insert_into(into_path, client_query):
    phrases = client_query.split(";")
    if len(phrases) > 1:
        for index in range(len(phrases) - 1):
            phrases[index] += ";"
    phrases.insert(len(phrases) - 1, f"INSERT INTO {into_path}")
    return '\n\n'.join(phrases)

