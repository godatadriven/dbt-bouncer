"""Protocol that every dbt resource wrapper in dbt-bouncer must satisfy.

Adding a new resource type
--------------------------
When adding support for a new dbt resource type, create a Pydantic model
that satisfies ``ResourceWrapper`` and follow these steps:

1. Add the resource's wrapper class here (or in ``parsers_*.py``) with
   at least ``unique_id: str`` and ``original_file_path: str``.
2. Add the new resource name to ``ResourceType`` in ``resource_type.py``.
3. Add the resource list to ``BouncerContext`` in ``context.py``.
4. Add the resource to ``resource_map`` and ``parsed_data`` in ``runner.py``.
5. Add the resource field to ``BaseCheck`` in ``check_base.py``.
6. Add a ``_require_<resource>`` guard method to ``BaseCheck``.
7. Create a ``checks/<resource>/`` directory with the check modules.

Design notes
------------
All resource wrappers share ``unique_id`` and ``original_file_path``.
The inner dbt object is accessed via an attribute whose name matches the
resource type string (e.g. ``DbtBouncerModel.model``,
``DbtBouncerSource.source``). The sole exception is ``DbtBouncerExposure``,
whose inner field is named ``model`` for historical reasons.

Macros and unit tests do not currently have wrapper classes — they are
passed as raw dbt-artifacts-parser objects and iterated directly.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class ResourceWrapper(Protocol):
    """Structural protocol for dbt resource wrapper objects.

    Every class whose instances are stored in ``BouncerContext`` and
    iterated by ``runner()`` must satisfy this protocol.  Satisfying it
    requires no explicit registration — Python's structural subtyping
    makes any class with matching attributes a conforming implementation.

    Attributes:
        unique_id: The resource's fully-qualified dbt unique identifier
            (e.g. ``"model.my_project.my_model"``).
        original_file_path: Path to the source file that defines this
            resource, relative to the project root.

    """

    unique_id: str
    original_file_path: str
