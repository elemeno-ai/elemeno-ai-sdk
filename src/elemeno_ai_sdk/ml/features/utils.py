import string

import jwt


def decode_api_key(api_key: str, verify_signature: bool = False) -> str:
    return jwt.decode(jwt=api_key, algorithms=["RS256"], options={"verify_signature": verify_signature})


def get_user_account_from_api_key(api_key: str) -> str:
    decoded_api_key = decode_api_key(api_key)
    return decoded_api_key.get("account")


def parse_user_account(user_account: str) -> str:
    for punctuation in string.punctuation:
        user_account = user_account.replace(punctuation, "-")
    return user_account


def get_feature_server_url_from_api_key(api_key: str) -> str:
    user_account = get_user_account_from_api_key(api_key)
    user_account = parse_user_account(user_account)
    return f"https://feature-server-{user_account}.app.elemeno.ai"
