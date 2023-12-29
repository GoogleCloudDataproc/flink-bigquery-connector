"""Abstract class for table type.
"""

import abc
import datetime
import random
import string
import sys


def is_perfect_hour(datetime_obj):
    """Returns True if the datetime object is a perfect hour, False otherwise."""
    return (
        datetime_obj.minute == 0
        and datetime_obj.second == 0
        and datetime_obj.microsecond == 0
    )


class TableType(abc.ABC):
    """Abstract that defines various table types.
    These table types are used for the creation of different types of tables for
    the e2e tests.
    """

    def generate_entries(self, number_of_elements_in_array):
        return [
            sys.maxsize
        ] * number_of_elements_in_array  # Array of LONGS: entries_i

    def generate_record(self, number_of_levels, level_number=0):
        if level_number == number_of_levels:
            return {'level_' + str(level_number): {'value': self.generate_long()}}
        return {
            'level_'
            + str(level_number): self.generate_record(
                number_of_levels, level_number + 1
            )
        }

    def generate_string(self):
        return ''.join(
            random.choices(string.ascii_letters, k=random.randint(8, 10))
        )

    def generate_long(self):
        return random.choice(range(0, 10000000))

    def generate_timestamp(self, current_timestamp):
        """Method to generate a random datetime within the given hour.
        Args:
          current_timestamp: Date is generated within one hour of this timestamp.
        Returns:
          datetime object: Containing generated timestamp.
        """
        next_hour = current_timestamp + datetime.timedelta(hours=1)
        random_timestamp = random.randint(
            int(current_timestamp.timestamp()), int(next_hour.timestamp())
        )
        utc = datetime.timezone.utc
        random_timestamp_utc = datetime.datetime.fromtimestamp(
            random_timestamp, utc
        )
        # Check if the generated entry is a perfect hour.
        # Note: It is only for the case of hour based partitioning
        # (as created in our test).
        # If the values inserted are changed to DAY or any other partitioning,
        # the prevention of borderline entry generation needs to be
        # changed accordingly.
        while is_perfect_hour(random_timestamp_utc):
            # Keep on regenerating.
            random_timestamp = random.randint(
                int(current_timestamp.timestamp()), int(next_hour.timestamp())
            )
            utc = datetime.timezone.utc
            random_timestamp_utc = datetime.datetime.fromtimestamp(
                random_timestamp, utc
            )
        return random_timestamp_utc

    @abc.abstractmethod
    def write_rows(
        self,
        number_of_rows_per_batch,
        writer,
        partition_number,
        current_timestamp,
    ):
        pass


class LargeTable(TableType):
    """Inherits abstract class table_type.
    Overwrites the method write_rows() to add rows
    according to Bounded Table of O(GB) order
    """

    # Overriding abstract method for insertion to a large table.
    def write_rows(
        self,
        number_of_rows_per_batch,
        writer,
        partition_number,
        current_timestamp,
    ):
        for _ in range(number_of_rows_per_batch):
            writer.append({
                'name': self.generate_string(),
                'number': self.generate_long(),
                'ts': self.generate_timestamp(current_timestamp=current_timestamp),
            })


class LargeRowTable(TableType):
    """Inherits abstract class table_type.
    Overwrites the method write_rows() to add rows
    according to Bounded Table with rows O(MB) order
    """

    def __init__(self, number_of_columns, number_of_elements_in_array):
        self.number_of_columns = number_of_columns
        self.number_of_elements_in_array = number_of_elements_in_array

    # Overriding abstract method for insertion to Table with Large Row.
    def write_rows(
        self,
        number_of_rows_per_batch,
        writer,
        partition_number,
        current_timestamp,
    ):
        for _ in range(number_of_rows_per_batch):
            record = {
                'name': self.generate_string(),
                'number': self.generate_long(),
                'ts': self.generate_timestamp(current_timestamp),
            }
            for col_number in range(self.number_of_columns):
                record[f'entries_{col_number}'] = self.generate_entries(
                    self.number_of_elements_in_array
                )
            writer.append(record)


class NestedSchemaTable(TableType):
    """Inherits abstract class table_type.
    Overwrites the method write_rows() to add rows
    according to Bounded Table with 15 levels
    """

    def __init__(self, number_of_levels):
        self.number_of_levels = number_of_levels

    # Overriding abstract method for insertion to Nested Schema Table
    def write_rows(
        self,
        number_of_rows_per_batch,
        writer,
        partition_number,
        current_timestamp,
    ):
        for _ in range(number_of_rows_per_batch):
            record = self.generate_record(self.number_of_levels)
            record['name'] = self.generate_string()
            record['number'] = self.generate_long()
            record['ts'] = self.generate_timestamp(current_timestamp)
            writer.append(record)


class PartitionedTable(TableType):
    """Inherits abstract class table_type.
    Overwrites the method write_rows() to add rows
    according to Partitioned Table
    """

    def __init__(self, now_timestamp):
        self.now_timestamp = now_timestamp

    # Overriding abstract method for Insertion to Partitioned Table
    def write_rows(
        self,
        number_of_rows_per_batch,
        writer,
        partition_number,
        current_timestamp,
    ):
        offset_timestamp = self.now_timestamp + datetime.timedelta(
            hours=partition_number
        )

        # Write the specified number of rows.
        for _ in range(number_of_rows_per_batch):
            writer.append({
                'name': self.generate_string(),
                'number': self.generate_long(),
                'ts': self.generate_timestamp(offset_timestamp),
            })
