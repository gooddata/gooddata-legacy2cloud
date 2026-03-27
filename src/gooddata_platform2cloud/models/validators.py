# (C) 2026 GoodData Corporation
from pydantic import BaseModel, HttpUrl


class UrlValidator(BaseModel):
    """Validate an HTTP(S) URL"""

    url: HttpUrl
