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
from requests.api import head
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

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
class DbtCloudStream(HttpStream, ABC):
    """
    This class represents a stream output by the connector.
    """

    url_base = "https://cloud.getdbt.com/api/v2/"

    primary_key = None

    def next_page_token(
        self, response: requests.Response, stream_batch_count: int
    ) -> Optional[Mapping[str, Any]]:
        """
        In each response given by the dbt cloud API, we're able to retrieve total number of occurences for the requested object and the number of occurences returned in this batch.
        "pagination": {
            "count": 10,
            "total_count": 2012
        }
        Our strategy here is to sum up all batches count and enter exit condition if the sum of all batches count is equal or greater (shouldn't happen)
        than total count as received from API response.
        """
        batch_count = response.json().get("extra").get("pagination").get("count")
        total_count = response.json().get("extra").get("pagination").get("total_count")
        if stream_batch_count + batch_count >= total_count:
            return None
        return stream_batch_count + batch_count

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Same technique as base HttpStream but we should now pass the sum of all batches records count to next_page_token method
        """
        stream_state = stream_state or {}
        pagination_complete = False

        next_page_token = None
        stream_batch_count = 0
        while not pagination_complete:
            request_headers = self.request_headers(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            )
            request = self._create_prepared_request(
                path=self.path(
                    stream_state=stream_state,
                    stream_slice=stream_slice,
                    next_page_token=next_page_token,
                ),
                headers=dict(request_headers, **self.authenticator.get_auth_header()),
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
                data=self.request_body_data(
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

            next_page_token = self.next_page_token(response, stream_batch_count)
            if next_page_token:
                stream_batch_count = next_page_token
            else:
                pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        for rec in response.json().get("data"):
            yield rec


class Accounts(DbtCloudStream):
    """
    Accounts LIST endpoint on dbt Cloud V2 API
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "accounts"


class DbtCloudDependentStream(DbtCloudStream):
    """
    We need to get all most dbt endpoints, ids of parent items in order to create URL.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """
        We use a for loop to read records from all paths.
        """
        stream_state = stream_state or {}
        pagination_complete = False

        stream_batch_count = 0
        while not pagination_complete:
            request_headers = self.request_headers(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=stream_batch_count,
            )
            for my_path in self.path(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=stream_batch_count,
            ):
                request = self._create_prepared_request(
                    path=my_path,
                    headers=dict(
                        request_headers, **self.authenticator.get_auth_header()
                    ),
                    params=self.request_params(
                        stream_state=stream_state,
                        stream_slice=stream_slice,
                        next_page_token=stream_batch_count,
                    ),
                    json=self.request_body_json(
                        stream_state=stream_state,
                        stream_slice=stream_slice,
                        next_page_token=stream_batch_count,
                    ),
                )
                request_kwargs = self.request_kwargs(
                    stream_state=stream_state,
                    stream_slice=stream_slice,
                    next_page_token=stream_batch_count,
                )
                response = self._send_request(request, request_kwargs)
                yield from self.parse_response(
                    response, stream_state=stream_state, stream_slice=stream_slice
                )

            next_page_token = self.next_page_token(response, stream_batch_count)
            if next_page_token:
                stream_batch_count = next_page_token
            else:
                pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []


class Projects(DbtCloudDependentStream):
    """
    Project LIST endpoint on dbt Cloud V2 API
    """

    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        """
        Get ids from Accounts stream to yield paths using parent account ids.
        """
        accounts_stream = Accounts(authenticator=self.authenticator)
        for account_rec in accounts_stream.read_records(
            sync_mode=SyncMode.full_refresh
        ):
            account_id = account_rec.get("id")
            yield f"/accounts/{account_id}/projects/"


class Jobs(DbtCloudDependentStream):
    """
    Jobs LIST endpoint on dbt Cloud V2 API
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.

    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        accounts_stream = Accounts(authenticator=self.authenticator)
        for account_rec in accounts_stream.read_records(
            sync_mode=SyncMode.full_refresh
        ):
            account_id = account_rec.get("id")
            yield f"/accounts/{account_id}/jobs/"


class Runs(DbtCloudDependentStream):
    """
    Runs LIST endpoint on dbt Cloud V2 API
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.

    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        accounts_stream = Accounts(authenticator=self.authenticator)
        for account_rec in accounts_stream.read_records(
            sync_mode=SyncMode.full_refresh
        ):
            account_id = account_rec.get("id")
            yield f"/accounts/{account_id}/runs/"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:

        return {
            "include_related": ["trigger", "job", "repository", "environment"],
            "offset": next_page_token,
            "limit": 1000,
        }


# Basic incremental stream
class IncrementalDbtCloudStream(DbtCloudStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


# Source
class SourceDbtCloud(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        api_key = config["api_key"]
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {api_key}",
        }
        url = f"{DbtCloudStream.url_base}accounts/"
        try:
            session = requests.get(url=url, headers=headers)
            session.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        auth = TokenAuthenticator(
            token=config["api_key"]
        )  # Oauth2Authenticator is also available if you need oauth support
        return [
            Accounts(authenticator=auth),
            Projects(authenticator=auth),
            Jobs(authenticator=auth),
            Runs(authenticator=auth),
        ]
