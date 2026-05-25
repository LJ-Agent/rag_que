"""Unified result wrapper for QUE Engine service methods."""
from dataclasses import dataclass, Generic, TypeVar

T = TypeVar("T")


@dataclass
class QueResult(Generic[T]):
    success: bool = True
    data: T | None = None
    message: str = ""
    error_code: int = 0

    @staticmethod
    def ok(data: T = None, message: str = "ok") -> "QueResult[T]":
        return QueResult(success=True, data=data, message=message)

    @staticmethod
    def fail(message: str, error_code: int = 1) -> "QueResult":
        return QueResult(success=False, data=None, message=message, error_code=error_code)
