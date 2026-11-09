# (C) 2026 GoodData Corporation
"""
Verifies that a created computed attribute is made visible to all workspace
users. Cloud calls are mocked.
"""

from unittest.mock import MagicMock

from gooddata_legacy2cloud.backends.cloud.client import CloudClient

CA = {"data": {"id": "label.comp.abc", "type": "computedAttribute"}}
VIEW_ALL = [{"assigneeRule": {"type": "allWorkspaceUsers"}, "permissions": ["VIEW"]}]


def _response(status_code: int) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.ok = status_code < 400
    return response


def test_create_grants_view_to_all(cloud_client: CloudClient, mocker) -> None:
    post = mocker.patch.object(
        cloud_client, "_post", side_effect=[_response(201), _response(204)]
    )

    response = cloud_client.create_computed_attribute(CA)

    assert response.status_code == 201
    assert post.call_count == 2
    url, payload, headers = post.call_args_list[1].args
    assert url.endswith("/computedAttributes/label.comp.abc/managePermissions")
    assert payload == VIEW_ALL
    assert headers["Content-Type"] == "application/json"


def test_failed_create_does_not_grant(cloud_client: CloudClient, mocker) -> None:
    post = mocker.patch.object(cloud_client, "_post", return_value=_response(400))

    response = cloud_client.create_computed_attribute(CA)

    assert response.status_code == 400
    assert post.call_count == 1


def test_failed_grant_keeps_create_response(cloud_client: CloudClient, mocker) -> None:
    mocker.patch.object(
        cloud_client, "_post", side_effect=[_response(201), _response(500)]
    )

    response = cloud_client.create_computed_attribute(CA)

    assert response.status_code == 201
