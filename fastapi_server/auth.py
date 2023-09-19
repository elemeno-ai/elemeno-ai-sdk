from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN

API_KEY_NAME = "api_key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def api_key_auth(api_key: str = Depends(api_key_header)):
    if api_key != "your_secret_api_key":
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Invalid API Key"
        )
    return api_key
