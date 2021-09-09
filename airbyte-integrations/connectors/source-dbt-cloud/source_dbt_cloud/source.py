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
        yield from []

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        for rec in response.json().get("data"):
            yield rec


class Accounts(DbtCloudStream):
    """
    Accounts LIST endpoint on dbt Cloud V2 API
    """

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
    Most dbt Cloud API's endpoint should pass to their URL an account ID, e.g. https://cloud.getdbt.com/api/v2/accounts/{accountId}/jobs/.
    For these streams, path method will yield all paths for the passed API key, 1 path per account.
    """

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


class SourceDbtCloud(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Check if connection is successful and if API key passed is a valid API key
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
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = TokenAuthenticator(token=config["api_key"])
        return [
            Accounts(authenticator=auth),
            Projects(authenticator=auth),
            Jobs(authenticator=auth),
            Runs(authenticator=auth),
        ]
