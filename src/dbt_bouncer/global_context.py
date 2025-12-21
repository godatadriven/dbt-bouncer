from pathlib import PurePath


class BouncerContext:
    """Context object for dbt-bouncer."""

    def __init__(
        self, config_file_path: PurePath, custom_checks_dir: str | None
    ) -> None:
        """Initialize the context."""
        self.config_file_path = config_file_path
        self.custom_checks_dir = custom_checks_dir


_context: BouncerContext | None = None


def set_context(ctx: BouncerContext) -> None:
    """Set the global context."""
    global _context
    _context = ctx


def get_context() -> BouncerContext | None:
    """Get the global context.

    Returns:
        BouncerContext | None: The global context if set, else None.

    """
    return _context
