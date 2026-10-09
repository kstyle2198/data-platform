from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone


class BruteForceDetector:

    def __init__(
        self,
        threshold: int = 5,
        window_seconds: int = 60,
    ):

        self.threshold = threshold

        self.window = timedelta(
            seconds=window_seconds
        )

        self.failed_attempts = defaultdict(
            deque
        )

    def process(self, event: dict):

        if event.get("event_type") != "LOGIN_FAILED":
            return None

        username = event.get("username")

        if not username:
            return None

        timestamp = self._parse_timestamp(
            event["timestamp"]
        )

        attempts = self.failed_attempts[
            username
        ]

        attempts.append(timestamp)

        cutoff = timestamp - self.window

        while (
            attempts
            and attempts[0] < cutoff
        ):
            attempts.popleft()

        count = len(attempts)

        print(
            "[Detector] "
            f"user={username} "
            f"failed_attempts={count}"
        )

        if count < self.threshold:
            return None

        return {
            "username": username,
            "failed_attempts": count,
            "window_seconds": int(
                self.window.total_seconds()
            ),
            "ip_address": event.get(
                "ip_address"
            ),
            "client_id": event.get(
                "client_id"
            ),
            "realm": event.get(
                "realm"
            ),
        }

    @staticmethod
    def _parse_timestamp(value):

        if isinstance(value, datetime):

            timestamp = value

        else:

            timestamp = datetime.fromisoformat(
                value.replace(
                    "Z",
                    "+00:00"
                )
            )

        if timestamp.tzinfo is None:

            timestamp = timestamp.replace(
                tzinfo=timezone.utc
            )

        return timestamp