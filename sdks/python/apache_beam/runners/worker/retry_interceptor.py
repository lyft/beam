import abc
import logging
import time

from random import randint, random
from typing import Tuple, Optional

import grpc

_LOGGER = logging.getLogger(__name__)


class SleepingPolicy(abc.ABC):

    @abc.abstractmethod
    def sleep(self, attempt: int):
        """
        :param attempt: the number of retry (starting from zero)
        """
        assert attempt >= 0


class ExponentialBackoff(SleepingPolicy):

    def __init__(self,
        init_backoff_ms: int = 100,
        max_backoff_ms: int = 1500,
        backoff_multiplier: int = 2
    ):
        self.init_backoff = randint(0, init_backoff_ms)
        self.max_backoff = max_backoff_ms
        self.multiplier = backoff_multiplier

    def sleep(self, attempt: int):
        sleep_range = min(
            self.init_backoff * self.multiplier ** attempt, self.max_backoff
        )
        sleep_ms = randint(0, sleep_range)
        _LOGGER.info(f"Sleeping for {sleep_ms}")
        time.sleep(sleep_ms / 1000)


class RetryOnRpcErrorClientInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor
):
    def __init__(
        self,
        *,
        max_attempts: int,
        sleeping_policy: SleepingPolicy,
        status_for_retry: Optional[Tuple[grpc.StatusCode]] = None,
    ):
        self.max_attempts = max_attempts
        self.sleeping_policy = sleeping_policy
        self.status_for_retry = status_for_retry

    def _intercept(self, continuation, client_call_details,
        request_or_iterator
    ):

        for attempt in range(self.max_attempts):
            response = continuation(client_call_details, request_or_iterator)

            if random.random() < 0.05:
                _LOGGER.info(f"RM: response {response}")

            if isinstance(response, grpc.RpcError):

                if attempt == (self.max_attempts - 1):
                    return response

                _LOGGER.error(f'RM, on worker '
                              f'{client_call_details.metadata.worker_id} '
                              f'timeout {client_call_details.timeout} and '
                              f'error due to {response.code()}')
                if (
                    self.status_for_retry
                    and response.code() not in self.status_for_retry
                ):
                    return response

                self.sleeping_policy.sleep(attempt)
            else:
                return response

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return self._intercept(continuation, client_call_details, request)

    def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator):
        return self._intercept(continuation, client_call_details,
                               request_iterator)
