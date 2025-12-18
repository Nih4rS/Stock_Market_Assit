from __future__ import annotations


class SmassistError(Exception):
    """Base exception for Stock Market Assist."""


class ConfigError(SmassistError):
    pass


class DataFetchError(SmassistError):
    pass


class StrategyError(SmassistError):
    pass


class ExcelIOError(SmassistError):
    pass
