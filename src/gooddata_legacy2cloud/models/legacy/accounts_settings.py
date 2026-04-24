# (C) 2026 GoodData Corporation
from gooddata_legacy2cloud.models.base import Base


class AccountSetting(Base):
    """Minimal account settings model, API response contains extra fields."""

    email: str
    login: str


class AccountSettingsWrapper(Base):
    """Wrapper for account settings."""

    account_setting: AccountSetting
