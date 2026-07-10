if __package__:
    from .adapter import register
else:  # pragma: no cover - direct module loading/test collection
    from adapter import register

__all__ = ["register"]
