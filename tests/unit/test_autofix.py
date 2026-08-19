"""Unit tests for dbt_bouncer.autofix."""

from types import SimpleNamespace

from dbt_bouncer.autofix import (
    PlannedFix,
    apply_fixes,
    plan_fixes,
    resolve_patch_file,
)

PROPERTIES_YAML = """models:
  # Keep this comment: round-trip editing must preserve it.
  - name: orders
    description: All orders.
  - name: customers
    access: private
"""


def _model(name="orders", tags=None, patch_path="proj://models/_models.yml"):
    return SimpleNamespace(name=name, patch_path=patch_path, tags=tags or [])


def _failure(check, model, run_id="check:0:resource"):
    return {
        "check": check,
        "check_run_id": run_id,
        "resource": SimpleNamespace(model=model),
    }


def _tags_check(tags, criteria="all"):
    return SimpleNamespace(criteria=criteria, name="check_model_has_tags", tags=tags)


def _access_check(access="public"):
    return SimpleNamespace(access=access, name="check_model_access")


class TestResolvePatchFile:
    """Tests for patch_path resolution."""

    def test_resolves_existing_file(self, tmp_path):
        """A patch_path resolves relative to the project directory."""
        patch_file = tmp_path / "models" / "_models.yml"
        patch_file.parent.mkdir()
        patch_file.write_text(PROPERTIES_YAML)

        assert resolve_patch_file(_model(), tmp_path) == patch_file

    def test_returns_none_without_patch_path(self, tmp_path):
        """A model without a properties-file entry cannot be fixed."""
        assert resolve_patch_file(_model(patch_path=None), tmp_path) is None

    def test_returns_none_for_missing_file(self, tmp_path):
        """A patch_path pointing at a nonexistent file cannot be fixed."""
        assert resolve_patch_file(_model(), tmp_path) is None


class TestPlanFixes:
    """Tests for fix planning."""

    def test_unfixable_check_is_skipped_with_reason(self, tmp_path):
        """A check without a registered fixer is reported, not fixed."""
        check = SimpleNamespace(name="check_model_description_populated")

        planned, skipped = plan_fixes([_failure(check, _model())], tmp_path)

        assert planned == []
        assert "no autofix" in skipped[0].reason

    def test_tags_criteria_other_than_all_is_skipped(self, tmp_path):
        """Choosing which tag satisfies criteria 'any' is a judgement call."""
        patch_file = tmp_path / "models" / "_models.yml"
        patch_file.parent.mkdir()
        patch_file.write_text(PROPERTIES_YAML)

        planned, skipped = plan_fixes(
            [_failure(_tags_check(["a", "b"], criteria="any"), _model())], tmp_path
        )

        assert planned == []
        assert "judgement call" in skipped[0].reason

    def test_missing_properties_file_is_skipped(self, tmp_path):
        """A model without a properties file yields a skip, not a crash."""
        planned, skipped = plan_fixes(
            [_failure(_tags_check(["a"]), _model(patch_path=None))], tmp_path
        )

        assert planned == []
        assert "properties-file" in skipped[0].reason


class TestApplyFixes:
    """Tests for fix application."""

    def _plan(self, tmp_path, failures):
        patch_file = tmp_path / "models" / "_models.yml"
        patch_file.parent.mkdir(exist_ok=True)
        patch_file.write_text(PROPERTIES_YAML)
        planned, skipped = plan_fixes(failures, tmp_path)
        assert skipped == []
        return patch_file, planned

    def test_appends_tags_and_preserves_comments(self, tmp_path):
        """Tags land under config.tags and the file's comment survives."""
        patch_file, planned = self._plan(
            tmp_path, [_failure(_tags_check(["daily"]), _model())]
        )

        applied, skipped = apply_fixes(planned)

        assert [f.check_run_id for f in applied] == ["check:0:resource"]
        assert skipped == []
        content = patch_file.read_text()
        assert "Keep this comment" in content
        assert "daily" in content

    def test_sets_access(self, tmp_path):
        """The access fixer writes the required access level."""
        patch_file, planned = self._plan(
            tmp_path, [_failure(_access_check("protected"), _model("customers"))]
        )

        applied, _ = apply_fixes(planned)

        assert len(applied) == 1
        assert "access: protected" in patch_file.read_text()

    def test_dry_run_writes_nothing(self, tmp_path):
        """Dry run reports the fix but leaves the file untouched."""
        patch_file, planned = self._plan(
            tmp_path, [_failure(_tags_check(["daily"]), _model())]
        )

        applied, _ = apply_fixes(planned, dry_run=True)

        assert len(applied) == 1
        assert patch_file.read_text() == PROPERTIES_YAML

    def test_entry_not_found_is_skipped(self, tmp_path):
        """A model absent from the properties file yields a skip."""
        patch_file, planned = self._plan(
            tmp_path, [_failure(_tags_check(["daily"]), _model("not_in_file"))]
        )

        applied, skipped = apply_fixes(planned)

        assert applied == []
        assert "no matching entry" in skipped[0].reason
        assert patch_file.read_text() == PROPERTIES_YAML

    def test_duplicate_fix_reports_already_present(self, tmp_path):
        """Two versions of one model share an entry; the second fix is a noop."""
        patch_file, planned = self._plan(
            tmp_path,
            [
                _failure(_tags_check(["daily"]), _model(), run_id="check:0:v1"),
                _failure(_tags_check(["daily"]), _model(), run_id="check:0:v2"),
            ],
        )

        applied, skipped = apply_fixes(planned)

        assert skipped == []
        assert len(applied) == 2
        assert "already present" in applied[1].description
        assert patch_file.read_text().count("daily") == 1


def test_planned_fix_dataclass_fields():
    """PlannedFix exposes the fields the CLI renders."""
    fix = PlannedFix(
        check_run_id="a", description="b", file=None, mutate=lambda _doc: "noop"
    )

    assert fix.description == "b"
