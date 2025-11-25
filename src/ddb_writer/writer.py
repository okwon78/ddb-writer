# pylint: disable=c-extension-no-member, too-many-statements

import time
import math
import json
import random
import logging
from datetime import datetime
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError
from botocore.config import Config

from pyspark.sql.functions import to_json, struct
from retry import retry


def put_items_to_dynamodb(
    table_name,
    tries,
    delay,
    backoff,
):
    sleep_time = random.random() * 5

    retry_config = Config(
        retries={
            "max_attempts": 1,  # 최대 재시도 횟수
            "mode": "adaptive",  # 재시도 모드 ('standard' 또는 'adaptive')
        }
    )

    def _json_to_dynamodb(json_data):
        if isinstance(json_data, dict):
            return {k: _json_to_dynamodb(v) for k, v in json_data.items()}
        if isinstance(json_data, list):
            return [_json_to_dynamodb(i) for i in json_data]
        if isinstance(json_data, float):
            return Decimal(str(json_data))

        return json_data

    class ThroughputException(Exception):
        def __init__(self, message):
            super().__init__(message)
            self.message = message

        def __str__(self):
            return self.message

    def dynamic_backoff(sleep_time, max_sleep=3.0, acceleration=5, fixed_delay=0.01):
        """
        지연 시간을 동적으로 증가시키는 함수입니다.

        Args:
            sleep_time (float): 현재 지연 시간입니다.
            max_sleep (float): 최대 허용 지연 시간입니다. 기본값은 3.0초입니다.
            acceleration (float): 지연 시간 증가에 사용되는 가속도입니다. 1 이상이어야 합니다. 기본값은 3입니다.
            fixed_delay (float): 지연 시간에 추가되는 고정 지연 시간입니다. 기본값은 0.2초입니다.

        Returns:
            float: 조정된 지연 시간으로, 최대 지연 시간을 초과하지 않습니다.

        Raises:
            ValueError: acceleration이 1 미만인 경우 발생합니다.
        """
        if acceleration < 1 or max_sleep < 1.0 or fixed_delay < 0:
            raise ValueError("Invalid arguments")

        number = max(0.001, sleep_time) * acceleration + fixed_delay
        sleep_time = (
            min(max_sleep, math.floor(number * 1000) / 1000)
            if sleep_time < max_sleep
            else max_sleep
        )

        time.sleep(sleep_time)
        return sleep_time

    def backoff_decay(sleep_time, min_sleep=0, decay_ratio=0.95, fixed_decrement=0.01):
        """
        백오프 지연 시간을 감소시키는 함수입니다.

        Args:
            sleep_time (float): 현재 지연 시간.
            min_sleep (float): 최소 지연 시간.
            decay_ratio (float): 지연 시간을 감소시키는 비율 (0 <= decay_ratio < 1).
            fixed_decrement (float): 지연 시간에서 고정적으로 차감할 값.

        Returns:
            float: 조정된 지연 시간.

        Raises:
            ValueError: decay_ratio가 1 이상인 경우 발생합니다.
        """

        if not 0 < decay_ratio < 1:
            raise ValueError("decay_ratio는 1 미만이어야 합니다.")

        number = sleep_time * decay_ratio - fixed_decrement
        sleep_time = max(min_sleep, math.floor(number * 1000) / 1000)
        
        if sleep_time:
            time.sleep(sleep_time)

        return sleep_time

    def log(message):
        ts = datetime.now().strftime('%d/%m/%y %H:%M:%S')
        print(f"[{ts}] {message}")
    
    @retry(
        exceptions=ThroughputException,
        tries=tries,
        delay=delay,
        backoff=backoff,
        jitter=(10, 200),
    )
    def _put_batch_items(ddb, requests):
        nonlocal sleep_time
    
        try:
            response = ddb.batch_write_item(
                RequestItems={table_name: requests}
            )

            unprocessed_items = response.get("UnprocessedItems", {}).get(
                table_name, None
            )

            if unprocessed_items:
                log(f"unprocessed_items: {len(unprocessed_items)}/{len(requests)}")
                sleep_time = dynamic_backoff(sleep_time)
                return unprocessed_items
            
            sleep_time = backoff_decay(sleep_time)
            return []

        except ClientError as exc:
            retry_exceptions = (
                "ProvisionedThroughputExceededException",
                "ThrottlingException",
            )
            error_code = exc.response["Error"]["Code"]

            if error_code in retry_exceptions:
                sleep_time = dynamic_backoff(sleep_time, fixed_delay=1)
                log(f"{error_code}")
                raise ThroughputException("Throughput limit exceeded. Please increase DynamoDB write capacity or adjust the number of EMR executors.") from exc

            log(f"Unknown DynamoDB 오류 발생: {exc}")
            raise

    def _inner_func(iterator):
        ddb = boto3.client(
            "dynamodb", region_name="ap-northeast-2", config=retry_config
        )

        serializer = TypeSerializer()

        requests = []
        for row in iterator:
            items = row.asDict()
            item = json.loads(items["json"])

            ddb_item = {
                k: serializer.serialize(_json_to_dynamodb(v))
                for k, v in item.items()
            }

            requests.append({"PutRequest": {"Item": ddb_item}})
            while len(requests) == 25:
                requests = _put_batch_items(ddb, requests=requests)

        while requests:
            requests = _put_batch_items(ddb, requests=requests)

    return _inner_func


class DynamoDBWriter:
    def __init__(self, table, write_capacity=None):
        self.dynamodb = boto3.client("dynamodb", region_name="ap-northeast-2")
        self.table = table

        status = self.get_table_status()
        self.current_write_capacity = status["WriteCapacityUnits"]
        self.current_read_capacity = status["ReadCapacityUnits"]

        self.logger = logging.getLogger("flo-reco")

        self.logger.info(">> table_name: %s", self.table)
        self.logger.info(">> current_read_capacity: %d", self.current_read_capacity)
        self.logger.info(">> current_write_capacity: %d", self.current_write_capacity)

        self.write_capacity = (
            self.current_write_capacity if write_capacity is None else write_capacity
        )

    def __enter__(self):
        self.logger.info(
            "Capacity Change Request write_capacity: %d",
            self.write_capacity,
        )

        self.update_write_capacity(write_capacity=self.write_capacity)

        return self

    def __exit__(self, *args, **kwargs):
        self.update_write_capacity(
            write_capacity=1,
        )

    def write_dynamodb(self, dataframe, tries = 10, delay = 0.5, backoff = 2):
        """
        대기_시간=delay * (backoff)^시도_횟수
        backoff 값이 1일 경우: 대기 시간이 늘어나지 않고 항상 일정합니다 (delay 시간만큼 고정 대기).
        backoff 값이 2일 경우: 대기 시간이 매번 2배씩 늘어납니다.
        """
        self.logger.info("tries: %s", tries)
        self.logger.info("delay: %s", delay)
        self.logger.info("backoff: %s", backoff)

        json_df = dataframe.select(to_json(struct(*dataframe.columns)).alias("json"))
        json_df.rdd.foreachPartition(
            put_items_to_dynamodb(
                self.table,
                tries=tries,
                delay=delay,
                backoff=backoff,
            )
        )

    def get_table_status(self):
        """get capacity on table"""
        response = self.dynamodb.describe_table(TableName=self.table)
        return {
            "TableStatus": response["Table"]["TableStatus"],
            "ReadCapacityUnits": response["Table"]["ProvisionedThroughput"][
                "ReadCapacityUnits"
            ],
            "WriteCapacityUnits": response["Table"]["ProvisionedThroughput"][
                "WriteCapacityUnits"
            ],
        }

    def _check_precondition(self, retries=50, interval=60):
        i = 0
        while True:
            i += 1
            if retries == i:
                raise RuntimeError(
                    f"""
                    >> Failed to update {self.table} status
                    >> because the retry count[{i}] exceeds the limit.
                    """
                )
            status = self.get_table_status()
            table_status = status["TableStatus"]

            if table_status == "ACTIVE":
                break

            self.logger.info(
                "Waiting for updating %s current status is %s",
                self.table,
                table_status,
            )

            time.sleep(interval * i)

    def _check_postcondition(self, write_capacity, retries=10, interval=60):
        # wait for update request to be completed
        i = 0
        while True:
            i += 1
            if retries == i:
                raise RuntimeError(
                    f"""
                    >> Failed to update {self.table} status
                    because the retry count[{i}] exceeds the limit
                    """
                )

            status = self.get_table_status()

            if (
                status["TableStatus"] == "ACTIVE"
                and status["WriteCapacityUnits"] == write_capacity
            ):
                self.logger.info(
                    "Success to update dynamodb [%s] status: %s",
                    self.table,
                    status,
                )
                return

            # wait for status to be active and meet requested target capacity
            # It mignt take serveral minutes.
            self.logger.info(
                "[%d] try to check update_capacity for %s.  status: %s",
                i,
                self.table,
                status,
            )
            time.sleep(interval * i)

    def update_write_capacity(self, write_capacity) -> None:
        """
        DynamoDB WriteCapacity
        """
        self._check_precondition()

        # When a LimitExceededException occurs,
        # DDB is designed to resume scale down operation after one hour.
        max_retries = 30
        delay = 5 * 60

        for attempt in range(max_retries):
            try:
                status = self.get_table_status()
                current_read_capacity = status["ReadCapacityUnits"]
                current_write_capacity = status["WriteCapacityUnits"]

                self.logger.info("Current_read_capacity: %s", current_read_capacity)
                self.logger.info("Current_write_capacity: %s", current_write_capacity)
                self.logger.info("Target_write_capacity: %s", write_capacity)

                if current_write_capacity == write_capacity:
                    return

                self.dynamodb.update_table(
                    TableName=self.table,
                    ProvisionedThroughput={
                        "ReadCapacityUnits": current_read_capacity,
                        "WriteCapacityUnits": write_capacity,
                    },
                )
                break  # Exit loop if update is successful
            except ClientError as err:
                if attempt >= max_retries:
                    raise

                if err.response["Error"]["Code"] == "LimitExceededException":
                    self.logger.warning(
                        "LimitExceededException: retrying in %d seconds (attempt %d/%d)",
                        delay,
                        attempt + 1,
                        max_retries,
                    )
                    time.sleep(delay)
                else:
                    self.logger.warning("Unexpected ClientError : %s", err)
                    raise

        self._check_postcondition(write_capacity)
