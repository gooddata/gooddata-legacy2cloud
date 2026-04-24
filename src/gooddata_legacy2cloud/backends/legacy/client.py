# (C) 2026 GoodData Corporation


import json
import logging
import time
from json import JSONDecodeError
from typing import Any

import requests

from gooddata_legacy2cloud.backends.legacy.validate_attribute_elements import (
    ValidateAttributeElements,
)
from gooddata_legacy2cloud.constants import LEGACY_REQUEST_PAGE_SIZE
from gooddata_legacy2cloud.models.legacy.analytical_dashboards import (
    AnalyticalDashboardWrapper,
)
from gooddata_legacy2cloud.models.legacy.metadata import Metadata
from gooddata_legacy2cloud.models.legacy.scheduled_exports import ScheduledMail
from gooddata_legacy2cloud.models.legacy.used_by import Entries, QueryResultWrapper
from gooddata_legacy2cloud.models.validators import UrlValidator

logger = logging.getLogger("migration")

USER_AGENT = "Migration_Tool/1.0"
TIMEOUT = 60


class LegacyClient:
    """A class used to interact with GD Legacy server."""

    def __init__(self, domain, pid, login, password):
        """
        Constructs a LegacyClient object with the given
        domain, project ID, login, and password.
        """
        self.domain = UrlValidator(url=domain).url.__str__()
        self.pid = pid
        self.login = login
        self.password = password
        self.user_id = None
        self.auth_sst = None
        self.cache = {}
        self.cache_attribute_elements = {}
        self.headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def initialize_attribute_elements_cache(self):
        """
        Initialize the attribute elements cache that can be used to get the attribute elements for missing values.
        """
        logger.info("----Validating Legacy Workspace----")
        validated_obj_response = self.get_validated_attribute_elements()
        validated_attr_elements = ValidateAttributeElements(
            validated_obj_response, self.domain, self.get_object
        )
        self.cache_attribute_elements = validated_attr_elements.get_objects_for_cache()

        # Get and display statistics
        warnings_count, cached_values_count = validated_attr_elements.get_statistics()
        logger.info(
            "%s warnings received; %s element values stored",
            warnings_count,
            cached_values_count,
        )

    def _set_super_secured_token(self):
        """
        Sets the "long-lived" SuperSecured Token (SST) valid for 7 days
        See https://help.gooddata.com/doc/growth/en/expand-your-gooddata-legacy/api-reference/#section/Use-Cases/Log-in
        """
        url = f"{self.domain}gdc/account/login"
        response = self._post(
            url=url,
            data={
                "postUserLogin": {
                    "login": self.login,
                    "password": self.password,
                    "verify_level": 2,
                }
            },
        )
        # If the response is not successful, raise an exception
        if response.status_code == 403:
            raise Exception(f"Access denied - {self.domain}")

        # Save the SSToken from the response
        self.auth_sst = response.json().get("userLogin", {}).get("token", None)

        # Save user ID of current user
        self.user_id = (
            response.json().get("userLogin", {}).get("profile", "").split("/")[-1]
        )
        if not self.user_id:
            raise Exception("Failed to retrieve user ID ")

        if self.auth_sst is None:
            raise Exception("Authentication failed. Please check your credentials.")

    def _set_temporary_token(self):
        """
        Set "short-lived" temporary token (TT) valid for 10 minutes
        """
        # Authenticate using the SSToken if it is not already set
        if self.auth_sst is None:
            self._set_super_secured_token()

        # Receive the AuthTT token (headers must contain AuthSST)
        response = requests.get(
            url=f"{self.domain}gdc/account/token",
            headers={**self.headers, **{"X-GDC-AuthSST": self.auth_sst}},
        )
        authTToken = response.json().get("userToken", {}).get("token", None)
        if authTToken is None:
            raise Exception("Authentication failed. Please check your credentials.")

        # Update common headers with the AuthTT token
        self.headers.update({"X-GDC-AuthTT": authTToken})

    def logout(self):
        """
        Logout current user (based on TT token)
        """
        # Logout the user
        if self.auth_sst is not None and self.user_id is not None:
            self._delete(
                url=f"{self.domain}gdc/account/login/{self.user_id}",
                custom_headers={
                    "X-GDC-AuthSST": self.auth_sst
                },  # Headers must contain AuthSST
            )
        logger.info("Logged out successfully.")

    def _get(
        self,
        url: str,
        return_original_response: bool = False,
        custom_headers: dict[str, str] | None = None,
    ) -> Any:
        # If the response is in cache, return it
        if url in self.cache:
            return self.cache[url]

        headers = {**self.headers, **(custom_headers or {})}
        response = requests.get(
            url,
            headers=headers,
            timeout=TIMEOUT,
        )
        if response.status_code == 401:
            # If the response is unauthorized, get a new token and try again
            self._set_temporary_token()
            # Rebuild headers with the new authentication token
            headers = {**self.headers, **(custom_headers or {})}
            response = requests.get(
                url,
                headers=headers,
                timeout=TIMEOUT,
            )

        # in case user wants to get the original response, return it
        if return_original_response:
            return response

        # If the response is not successful, raise an exception
        if response.status_code == 403:
            raise Exception(f"Access denied - {self.domain}")

        # Store the response in cache
        self.cache[url] = response.json()
        return response.json()

    def _post(self, url, data, custom_headers=None):
        """Sends a POST request to the server with a given JSON object."""
        headers = {**self.headers, **(custom_headers or {})}
        data_json = json.dumps(data)
        response = requests.post(
            url,
            data=data_json,
            headers=headers,
            timeout=TIMEOUT,
        )

        if response.status_code == 401:
            # If the response is unauthorized, get a new token and try again
            self._set_temporary_token()
            # Rebuild headers with the new authentication token
            headers = {**self.headers, **(custom_headers or {})}
            response = requests.post(
                url,
                data=data_json,
                headers=headers,
                timeout=TIMEOUT,
            )
        if response.status_code == 403:
            raise Exception(f"Access denied - {self.domain}")
        if response.status_code == 401:
            raise Exception(f"Unauthorized - {self.domain}")
        return response

    def _delete(self, url, custom_headers=None):
        """Sends a DELETE request to the server."""
        response = requests.delete(
            url,
            headers={**self.headers, **(custom_headers or {})},
            timeout=TIMEOUT,
        )

        if response.status_code == 401:
            # If the response is unauthorized, get a new token and try again
            self._set_temporary_token()
            response = requests.delete(
                url,
                headers={**self.headers, **(custom_headers or {})},
                timeout=TIMEOUT,
            )
        if response.status_code == 403:
            raise Exception(f"Access denied - {self.domain}")
        return response

    def get_objects_by_category(
        self,
        category,
        message_prefix="objects",
        page=0,
        total_objects_count=0,
        all_objects=None,
    ):
        """
        Generic method to retrieve definition of all Legacy objects of a particular category.
        Handles pagination of API calls. Does not filter objects, just retrieves all of them.

        Args:
            category (str): The object category (e.g., 'metric', 'analyticalDashboard')
            message_prefix (str): The prefix for progress message
            page (int): The current page number (used for recursion)
            total_objects_count (int): Total number of objects (used in recursion)
            all_objects (list): List of all objects from all pages (used in recursion)

        Returns:
            list: A list of all objects of the specified category
        """
        # Initialize all_objects list if not provided
        if all_objects is None:
            all_objects = []
        offset = page * LEGACY_REQUEST_PAGE_SIZE
        url = f"{self.domain}/gdc/md/{self.pid}/objects/query?category={category}&limit={LEGACY_REQUEST_PAGE_SIZE}&offset={offset}&deprecated=1"

        try:
            if page == 0:
                logger.info("Fetching all %s.", message_prefix)

            result = self._get(url)
            objects = result["objects"]["items"]

            # Add objects to the collection
            all_objects.extend(objects)

            # If there are more pages, get them
            if len(objects) == LEGACY_REQUEST_PAGE_SIZE:
                self.get_objects_by_category(
                    category, message_prefix, page + 1, total_objects_count, all_objects
                )

            # Only finish the line when we're at the base recursion level
            if page == 0:
                logger.info(" Done (%s %s fetched)", len(all_objects), message_prefix)
            return all_objects

        except KeyError:
            if page == 0:
                logger.error(" Failed")
            return []

    def get_ldm_api(self):
        """
        Sends a GET request to the server and returns a poll link.
        """
        url = (
            f"{self.domain}/gdc/projects/{self.pid}/model/view"
            "?includeGrain=true&excludeFactRule=true"
            "&includeNonProduction=false&includeCA=false&includeDeprecated=true"
        )
        json_data = self._get(url)
        poll = json_data["asyncTask"]["link"]["poll"]
        return poll

    def get_ldm(self, ldm_api):
        """
        Sends a GET request to the server with a given API and returns
        the response as a JSON object.
        """
        url = self.domain + ldm_api
        return self._get(url)

    def get_model(self):
        """Retrieves and processes the Legacy Logical Data Model (LDM)."""
        ldm_api = self.get_ldm_api()

        # review whether it is necessary to wait for the async task to be ready
        # alternatively implement a loop to check the status of the async task
        time.sleep(5)
        return self.get_ldm(ldm_api)

    def get_dataset_mappings(self):
        """Retrieves available dataset mappings."""
        url = f"{self.domain}/gdc/dataload/internal/projects/{self.pid}/modelMapping/datasets"
        return self._get(url)

    def get_output_stage(self):
        """Retrieves the output stage."""
        url = f"{self.domain}/gdc/dataload/projects/{self.pid}/outputStage"
        return self._get(url)

    def get_object(self, obj_link: str):
        """Retrieves a Legacy object by it's URI."""
        url = self.domain + obj_link
        return self._get(url)

    def get_object_by_identifier(self, identifier):
        """Retrieves a Legacy object by its identifier."""
        url = f"{self.domain}/gdc/md/{self.pid}/obj/identifier:{identifier}"
        return self._get(url)

    def fetch_valid_elements(
        self, display_form_id: str, element_uris: list[str]
    ) -> dict[str, Any]:
        """
        Fetch element values in batch using the validElements endpoint.

        This method fetches multiple element values in a single API call,
        which is more efficient than fetching them individually.

        Args:
            display_form_id: The display form object ID (e.g., "634")
            element_uris: List of element URIs to fetch (up to 50)

        Returns:
            dict: The validElements response with element values

        Example:
            >>> legacy_client.fetch_valid_elements("634", [
            ...     "/gdc/md/workspace/obj/633/elements?id=2013",
            ...     "/gdc/md/workspace/obj/633/elements?id=2014"
            ... ])
        """
        url = f"{self.domain}/gdc/md/{self.pid}/obj/{display_form_id}/validElements?orderKey=pk"
        payload = {
            "validElementsRequest": {
                "uris": element_uris,
                "complement": "0",
                "includeTotalCountWithoutFilters": "0",
            }
        }

        response = self._post(url, payload)
        return response.json()

    def get_metrics(self):
        """
        Retrieves a list of Legacy metrics.

        Returns:
            list: A list of metrics
        """
        metrics = self.get_objects_by_category("metric", "metrics")
        logger.info("Retrieved %s metrics", len(metrics))
        return metrics

    @staticmethod
    def validate_legacy_scheduled_exports(exports: list[Any]) -> list[ScheduledMail]:
        """Validate Legacy scheduled exports against schema."""
        validated_exports = [
            ScheduledMail(**export["scheduledMail"]) for export in exports
        ]

        return validated_exports

    def get_prompt_project_object(self, prompt_link):
        """
        Retrieves a Legacy prompt value.
        All prompts are available at
            /gdc/md/WORKSPACE_ID/objects/query?category=prompt&limit=50&offset=0&deprecated=1
        We are interested in prompt.content.type ='scalar'
        """
        url = f"{self.domain}/gdc/md/{self.pid}/variables/search"
        payload = {
            "variablesSearch": {
                "variables": [prompt_link],
                "context": [],
                "searchOptions": {"offset": 0, "limit": 500},
            }
        }
        payload_len = len(json.dumps(payload))
        headers = {
            "Content-Length": str(payload_len),
        }

        response = self._post(url, payload, headers)
        json_response = response.json()
        for item in json_response["variables"]:
            if item["level"] == "project":
                return item

        raise ValueError(f"Prompt 'project' does not exist - {prompt_link}")

    def get_insights(self):
        """
        Retrieves a list of Legacy insights.

        Returns:
            list: A list of insights
        """
        insights = self.get_objects_by_category("visualizationObject", "insights")
        logger.info("Retrieved %s insights", len(insights))
        return insights

    def get_insights_list(self):
        """Retrieves and processes the Legacy insights."""
        url = f"{self.domain}/gdc/md/{self.pid}/query/visualizationobjects?showAll=1"
        return self._get(url)

    def get_color_palette(self):
        """Retrieves the Legacy color palette."""
        url = f"{self.domain}/gdc/projects/{self.pid}/styleSettings"
        try:
            return self._get(url)
        except JSONDecodeError:
            return

    def get_dashboards(self, dashboard_type="analyticalDashboard"):
        """
        Returns a list of Legacy dashboards for the specified type.

        Args:
            dashboard_type (str): Dashboard type (default: 'analyticalDashboard').
            Examples: 'analyticalDashboard' for Legacy KPI, 'projectDashboard' for Legacy PixelPerfect.

        Returns:
            list: Dashboards of the given type.
        """
        dashboards = self.get_objects_by_category(dashboard_type, "dashboards")
        logger.info(
            "Retrieved %s dashboards of type %s", len(dashboards), dashboard_type
        )
        return dashboards

    def get_dashboard_objects(self) -> list[AnalyticalDashboardWrapper]:
        """
        Retrieves a list of Legacy KPI dashboards.

        Returns:
            list[AnalyticalDashboardWrapper]: A list of validated dashboard objects
        """
        raw_dashboards: list[dict] = self.get_dashboards()
        validated_dashboards: list[AnalyticalDashboardWrapper] = []
        for dashboard in raw_dashboards:
            validated_dashboards.append(AnalyticalDashboardWrapper(**dashboard))
        return validated_dashboards

    def get_attributes(self):
        """Retrieves workspace attributes."""
        url = f"{self.domain}/gdc/md/{self.pid}/query/attributes?showAll=1"
        return self._get(url)

    def get_facts(self):
        """Retrieves workspace facts."""
        url = f"{self.domain}/gdc/md/{self.pid}/query/facts?showAll=1"
        return self._get(url)

    def get_insights_query(self) -> QueryResultWrapper:
        """Retrieves workspace insights query."""
        url = f"{self.domain}/gdc/md/{self.pid}/query/visualizationobjects?showAll=1"
        raw_result = self._get(url)
        validated_objects = QueryResultWrapper(**raw_result)
        return validated_objects

    def get_validated_attribute_elements(self):
        """Validates project elements and returns the result."""
        poll_url = self._get_validated_attribute_elements_poll()
        if poll_url:
            return self._poll_url(poll_url)
        else:
            raise Exception("No poll URL found for validated objects")

    def _get_validated_attribute_elements_poll(self):
        """
        Initiates project element validation and returns a poll URL.

        A POST request is sent to the validation API. If the request is accepted
        and a task is created (HTTP 201), this method returns a URL that can be
        polled for the validation result.
        """
        api_url = f"{self.domain}/gdc/md/{self.pid}/validate/"
        payload = {"validateProject": ["pdm::elem_validation"]}

        # Calculate Content-Length as it was in the original snippet.
        # _post will use this in custom_headers.
        payload_len = len(json.dumps(payload))
        custom_headers = {
            "Content-Length": str(payload_len),
        }

        logger.info("Running workspace validation.")
        # Make the initial POST request
        post_response = self._post(api_url, payload, custom_headers=custom_headers)
        if post_response.status_code == 201:
            poll_link = post_response.json()["asyncTask"]["link"]["poll"]
            return f"{self.domain}{poll_link}"
        else:
            raise Exception(
                f"Initial POST to {api_url} failed with HTTP {post_response.status_code}"
            )

    def _poll_url(self, poll_url):
        """Polls the given URL until the response is ready."""
        while True:
            poll_response = self._get(poll_url, return_original_response=True)

            if poll_response.status_code == 200:
                logger.info(" Done")
                return poll_response.json()
            elif poll_response.status_code == 202:
                # Progress dots are logged at debug level
                time.sleep(5)
            else:
                raise Exception(
                    f"Polling URL {poll_url} returned status code {poll_response.status_code}"
                )

    def get_workspace_users(self, page: int = 0, all_users: list | None = None) -> list:
        """
        Retrieves a list of all Legacy users in the workspace with pagination.

        Args:
            page: The current page number (used for recursion)
            all_users: List of all users from all pages (used in recursion)

        Returns:
            A list of user dictionaries with profile information
        """
        if all_users is None:
            all_users = []

        offset = page * LEGACY_REQUEST_PAGE_SIZE
        url = f"{self.domain}/gdc/projects/{self.pid}/users/?offset={offset}&limit={LEGACY_REQUEST_PAGE_SIZE}"

        try:
            # Progress indicator
            if page == 0:
                logger.info("Fetching all workspace users.")
            # Progress dots are logged at debug level

            result = self._get(url)
            users = result.get("users", [])

            # Add users to the collection
            all_users.extend(users)

            # If we got a full page, there might be more
            if len(users) == LEGACY_REQUEST_PAGE_SIZE:
                return self.get_workspace_users(page + 1, all_users)

            # Only finish the line when we're at the base recursion level
            if page == 0 or len(users) < LEGACY_REQUEST_PAGE_SIZE:
                logger.info(" Done (%s users fetched)", len(all_users))

            return all_users

        except KeyError:
            if page == 0:
                logger.error(" Failed")
            return all_users

    def get_object_grantees(self, object_numerical_id: str) -> dict:
        """
        Retrieves grantees (users and groups with permissions) for a specific object.

        Args:
            object_numerical_id: The numerical ID of the object (from meta.uri)

        Returns:
            dict: Grantees response containing users and groups with permissions
        """
        url = (
            f"{self.domain}/gdc/projects/{self.pid}/obj/{object_numerical_id}/grantees"
        )
        try:
            return self._get(url)
        except Exception:
            # Return empty grantees structure if fetch fails
            return {"grantees": {"items": []}}

    def get_domain_metadata(self) -> Metadata:
        """
        Retrieves a list of all workspaces on the domain.
        """
        url = f"{self.domain}/gdc/md"
        raw_workspaces = self._get(url)
        validated_workspaces = Metadata.model_validate(raw_workspaces)
        return validated_workspaces

    def get_object_dependencies(self, object_uri: str) -> Entries:
        """
        Retrieves dependencies of a specific object.
        """

        # Extract numeric identifier from object URI
        object_id = object_uri.split("/obj/")[-1]
        types = "analyticalDashboard,attribute,fact,metric,projectDashboard,prompt,report,visualizationObject"
        url = f"{self.domain}/gdc/md/{self.pid}/usedby2/{object_id}?nearest=0&types={types}"
        raw_result = self._get(url)
        validated_result = Entries.model_validate(raw_result)
        return validated_result
