from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import boto3


log = logging.getLogger(__name__)


@dataclass
class SqsMessage:
    receipt_handle: str
    body: str
    message_id: str
    attributes: Dict[str, Any]


class SqsReader:
    def __init__(
        self,
        queue_url: str,
        region: Optional[str],
        wait_seconds: int,
        max_batch: int,
        visibility_timeout_seconds: int,
    ) -> None:
        self.queue_url = queue_url
        self.wait_seconds = wait_seconds
        self.max_batch = max(1, min(10, max_batch))
        self.visibility_timeout_seconds = visibility_timeout_seconds

        self.client = boto3.client("sqs", region_name=region) if region else boto3.client("sqs")

    def receive(self) -> List[SqsMessage]:
        resp = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.max_batch,
            WaitTimeSeconds=self.wait_seconds,
            VisibilityTimeout=self.visibility_timeout_seconds,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        msgs = resp.get("Messages", [])
        out: List[SqsMessage] = []
        for m in msgs:
            out.append(
                SqsMessage(
                    receipt_handle=m["ReceiptHandle"],
                    body=m.get("Body", ""),
                    message_id=m.get("MessageId", ""),
                    attributes=m.get("Attributes", {}),
                )
            )
        return out

    def delete(self, receipt_handle: str) -> None:
        self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
