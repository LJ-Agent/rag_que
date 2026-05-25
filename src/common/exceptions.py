"""Domain exceptions for QUE Engine."""


class QueException(Exception):
    def __init__(self, message: str, code: int = 1):
        super().__init__(message)
        self.message = message
        self.code = code


class IntentRecognitionException(QueException):
    def __init__(self, message: str = "Intent recognition failed"):
        super().__init__(message, code=1001)


class QueryRewritingException(QueException):
    def __init__(self, message: str = "Query rewriting failed"):
        super().__init__(message, code=1002)


class PlanningException(QueException):
    def __init__(self, message: str = "Query planning failed"):
        super().__init__(message, code=1003)


class ExecutionException(QueException):
    def __init__(self, message: str = "Sub-query execution failed"):
        super().__init__(message, code=2001)


class SynthesisException(QueException):
    def __init__(self, message: str = "Result synthesis failed"):
        super().__init__(message, code=3001)


class DownstreamException(QueException):
    def __init__(self, service: str, message: str = "Downstream service unavailable"):
        super().__init__(f"{service}: {message}", code=4001)
