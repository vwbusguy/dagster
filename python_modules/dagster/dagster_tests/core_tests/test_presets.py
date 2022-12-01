from dagster._legacy import PresetDefinition


def test_empty_preset():
    empty_preset = PresetDefinition("empty")
    assert empty_preset.run_config is None
    assert empty_preset.get_environment_yaml() == "{}\n"


def test_merge():
    base_preset = PresetDefinition(
        "base",
        run_config={
            "context": {
                "unittest": {
                    "resources": {
                        "db_resource": {
                            "config": {"user": "some_user", "password": "some_password"}
                        }
                    }
                }
            }
        },
    )

    new_preset = base_preset.with_additional_config(
        {"context": {"unittest": {"resources": {"another": {"config": "not_sensitive"}}}}}
    )

    assert new_preset.run_config == {
        "context": {
            "unittest": {
                "resources": {
                    "db_resource": {"config": {"user": "some_user", "password": "some_password"}},
                    "another": {"config": "not_sensitive"},
                }
            }
        }
    }
