class SendError(RuntimeError):
    """Помилка відправки на dev-сервер (обрив з'єднання, відмова handshake, таймаут)."""


class ApiError(RuntimeError):
    """Помилка API платформи."""

    def __init__(self, message, status=0):
        super().__init__(message)
        self.status = status


class FatalTransferError(RuntimeError):
    """Перенос неможливо продовжувати (сервер недоступний або нічого не приймає)."""
