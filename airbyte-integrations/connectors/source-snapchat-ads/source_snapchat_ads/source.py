#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http import auth
from airbyte_cdk.sources.streams.http.auth import (
    Oauth2Authenticator,
)
import logging

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.json file.
"""


# Basic full refresh stream
class SnapchatAdsStream(HttpStream):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class SnapchatAdsStream(HttpStream, ABC)` which is the current class
    `class Customers(SnapchatAdsStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(SnapchatAdsStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalSnapchatAdsStream((SnapchatAdsStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    # TODO: Fill in the url base. Required.
    url_base = "https://adsapi.snapchat.com/v1/"
    primary_key = None

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def path(self, **kwargs) -> str:
        return None

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        return None

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}


class Organizations(SnapchatAdsStream):
    def path(self, **kwargs) -> str:
        return "me/organizations"

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        records = []
        for rec in response.json()["organizations"]:
            records.append(rec["organization"])
        return records

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}


class AdAccounts(SnapchatAdsStream):
    def path(self, **kwargs) -> str:
        organizations_stream = Organizations(authenticator=self.authenticator)
        organization_id = next(
            organizations_stream.read_records(sync_mode=SyncMode.full_refresh)
        )["id"]
        return f"organizations/{organization_id}/adaccounts"

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        records = []
        for rec in response.json()["adaccounts"]:
            records.append(rec["adaccount"])
        return records

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}


class Campaigns(SnapchatAdsStream):
    def path(self, **kwargs) -> str:
        ad_accounts_stream = AdAccounts(authenticator=self.authenticator)
        for ad_account_record in ad_accounts_stream.read_records(
            sync_mode=SyncMode.full_refresh
        ):
            yield f"adaccounts/{ad_account_record['id']}/campaigns"

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        records = []
        for rec in response.json()["campaigns"]:
            records.append(rec["campaign"])
        return records

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        stream_state = stream_state or {}
        pagination_complete = False

        next_page_token = None
        while not pagination_complete:
            request_headers = self.request_headers(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            )
            for my_path in self.path(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ):
                request = self._create_prepared_request(
                    path=my_path,
                    headers=dict(
                        request_headers, **self.authenticator.get_auth_header()
                    ),
                    params=self.request_params(
                        stream_state=stream_state,
                        stream_slice=stream_slice,
                        next_page_token=next_page_token,
                    ),
                    json=self.request_body_json(
                        stream_state=stream_state,
                        stream_slice=stream_slice,
                        next_page_token=next_page_token,
                    ),
                )
                request_kwargs = self.request_kwargs(
                    stream_state=stream_state,
                    stream_slice=stream_slice,
                    next_page_token=next_page_token,
                )
                response = self._send_request(request, request_kwargs)
                yield from self.parse_response(
                    response, stream_state=stream_state, stream_slice=stream_slice
                )

            next_page_token = self.next_page_token(response)
            if not next_page_token:
                pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []


class AdSquads(SnapchatAdsStream):
    def path(self, **kwargs) -> str:
        ad_accounts_stream = AdAccounts(authenticator=self.authenticator)
        for ad_account_record in ad_accounts_stream.read_records(
            sync_mode=SyncMode.full_refresh
        ):
            yield f"adaccounts/{ad_account_record['id']}/adsquads"

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        records = []
        for rec in response.json()["adsquads"]:
            records.append(rec["adsquad"])
        return records

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        stream_state = stream_state or {}
        pagination_complete = False

        next_page_token = None
        while not pagination_complete:
            request_headers = self.request_headers(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            )
            for my_path in self.path(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ):
                request = self._create_prepared_request(
                    path=my_path,
                    headers=dict(
                        request_headers, **self.authenticator.get_auth_header()
                    ),
                    params=self.request_params(
                        stream_state=stream_state,
                        stream_slice=stream_slice,
                        next_page_token=next_page_token,
                    ),
                    json=self.request_body_json(
                        stream_state=stream_state,
                        stream_slice=stream_slice,
                        next_page_token=next_page_token,
                    ),
                )
                request_kwargs = self.request_kwargs(
                    stream_state=stream_state,
                    stream_slice=stream_slice,
                    next_page_token=next_page_token,
                )
                response = self._send_request(request, request_kwargs)
                yield from self.parse_response(
                    response, stream_state=stream_state, stream_slice=stream_slice
                )

            next_page_token = self.next_page_token(response)
            if not next_page_token:
                pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []


class AdSquadsStats(SnapchatAdsStream):
    def path(self, **kwargs) -> str:
        ad_squad_stream = AdSquads(authenticator=self.authenticator)
        for ad_squad_record in ad_squad_stream.read_records(
            sync_mode=SyncMode.full_refresh
        ):
            yield f"adsquads/{ad_squad_record['id']}/stats"

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        records = []
        for rec in response.json()["total_stats"]:
            records.append(rec["total_stat"])
        return records

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {
            "fields": "impressions,swipes,screen_time_millis,quartile_1,quartile_2,quartile_3,view_completion,spend"
        }

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        stream_state = stream_state or {}
        pagination_complete = False

        next_page_token = None

        while not pagination_complete:
            request_headers = self.request_headers(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            )
            for my_path in self.path(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ):
                request = self._create_prepared_request(
                    path=my_path,
                    headers=dict(
                        request_headers, **self.authenticator.get_auth_header()
                    ),
                    params=self.request_params(
                        stream_state=stream_state,
                        stream_slice=stream_slice,
                        next_page_token=next_page_token,
                    ),
                    json=self.request_body_json(
                        stream_state=stream_state,
                        stream_slice=stream_slice,
                        next_page_token=next_page_token,
                    ),
                )
                request_kwargs = self.request_kwargs(
                    stream_state=stream_state,
                    stream_slice=stream_slice,
                    next_page_token=next_page_token,
                )
                response = self._send_request(request, request_kwargs)
                yield from self.parse_response(
                    response, stream_state=stream_state, stream_slice=stream_slice
                )

            next_page_token = self.next_page_token(response)
            if not next_page_token:
                pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []


# Source
class SourceSnapchatAds(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            access_token = config["access_token"]
            refresh_token = config["refresh_token"]

            client_id = config["client_id"]
            client_secret = config["client_secret"]
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.

        authenticator = Oauth2Authenticator(
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            refresh_token=config["refresh_token"],
            token_refresh_endpoint="https://accounts.snapchat.com/login/oauth2/access_token",
        )

        # auth = NoAuth()
        # Oauth2Authenticator is also available if you need oauth support
        return [
            Organizations(
                authenticator=authenticator,
            ),
            AdAccounts(authenticator=authenticator),
            Campaigns(authenticator=authenticator),
            AdSquads(authenticator=authenticator),
            AdSquadsStats(authenticator=authenticator),
        ]
