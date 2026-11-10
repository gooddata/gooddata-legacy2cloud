# (C) 2026 GoodData Corporation
"""VIEW is granted on every deployed computed attribute except the failed ones."""

from pytest_mock import MockerFixture

from gooddata_legacy2cloud.workflows import migrate_metrics as workflow


def _ca(ca_id: str) -> dict:
    return {"data": {"id": ca_id, "type": "computedAttribute"}}


def test_failed_cas_excluded_from_grant(mocker: MockerFixture) -> None:
    cloud_client = mocker.MagicMock()
    cloud_client.grant_view_to_all_on_computed_attributes.return_value = []
    write = mocker.patch.object(workflow, "write_content_to_file")

    workflow.grant_view_on_computed_attributes(
        cloud_client, [_ca("new"), _ca("skipped"), _ca("broken")], [_ca("broken")]
    )

    cloud_client.grant_view_to_all_on_computed_attributes.assert_called_once_with(
        ["new", "skipped"]
    )
    write.assert_not_called()


def test_failed_grants_written_to_file(mocker: MockerFixture) -> None:
    cloud_client = mocker.MagicMock()
    cloud_client.grant_view_to_all_on_computed_attributes.return_value = ["x"]
    write = mocker.patch.object(workflow, "write_content_to_file")

    workflow.grant_view_on_computed_attributes(cloud_client, [_ca("x")], [])

    write.assert_called_once()
    assert write.call_args.args[0] == workflow.CLOUD_FAILED_CA_PERMISSIONS_FILE
    assert '"x"' in write.call_args.args[1]
