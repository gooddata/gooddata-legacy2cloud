# (C) 2026 GoodData Corporation
"""
Verifies the VIEW grant for all workspace users on computed attributes.
Cloud calls are mocked.
"""

from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from gooddata_legacy2cloud.backends.cloud.client import CloudClient

CA = {"data": {"id": "label.comp.abc", "type": "computedAttribute"}}
VIEW_ALL = [{"assigneeRule": {"type": "allWorkspaceUsers"}, "permissions": ["VIEW"]}]


def _response(status_code: int) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.ok = status_code < 400
    return response


def test_create_does_not_grant(
    cloud_client: CloudClient, mocker: MockerFixture
) -> None:
    post = mocker.patch.object(cloud_client, "_post", return_value=_response(201))

    cloud_client.create_computed_attribute(CA)

    assert post.call_count == 1


def test_grant_posts_view_all(cloud_client: CloudClient, mocker: MockerFixture) -> None:
    post = mocker.patch.object(cloud_client, "_post", return_value=_response(204))

    failed = cloud_client.grant_view_to_all_on_computed_attributes(["label.comp.abc"])

    assert failed == []
    url, payload, headers = post.call_args.args
    assert url.endswith("/computedAttributes/label.comp.abc/managePermissions")
    assert payload == VIEW_ALL
    assert headers["Content-Type"] == "application/json"


def test_grant_any_2xx_is_success(
    cloud_client: CloudClient, mocker: MockerFixture
) -> None:
    mocker.patch.object(cloud_client, "_post", return_value=_response(200))

    assert cloud_client.grant_view_to_all_on_computed_attributes(["a"]) == []


def test_grant_reports_failed_ids(
    cloud_client: CloudClient, mocker: MockerFixture
) -> None:
    mocker.patch.object(
        cloud_client,
        "_post",
        side_effect=[_response(500), ConnectionError("boom"), _response(204)],
    )

    failed = cloud_client.grant_view_to_all_on_computed_attributes(["a", "b", "c"])

    assert failed == ["a", "b"]
