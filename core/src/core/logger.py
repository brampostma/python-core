import abc
import logging
import sys
import uuid
from dataclasses import dataclass


@dataclass
class LoggingPayload:
    run_id: str
    source_name: str
    reporting_date: str  # format 2022-01-31
    count: str
    elapsed_seconds: str


class BaseLogger(logging.Logger):
    """
    A Logging interface configured to connect to Azure. Serves as an interface for other custom loggers.
    """

    def __init__(
        self,
        logger_id: str = "",
        logger_name: str = "",
        level: int = logging.INFO,
        log_format: str = "%(asctime)s %(levelname)-8s [%(logger_id)s] %(message)s",
        date_format: str = "%Y-%m-%d %H:%M:%S",
    ) -> None:
        """
        Initializes the BaseLogger with a specified configuration.

        :param logger_id: The run id that will be used to tag every log message.
        :param logger_name: The name of the logger.
        :param connection_string: The connection string for the Azure Application Insights resource.
        :param level: The logging level (default is logging.INFO).
        :param log_format: The format string for log messages.
        :param date_format: The format string for timestamps in log messages.
        """
        self.log_level = level
        self._verify_log_level()
        super().__init__(logger_name, self.log_level)

        self.logger_id = logger_id
        self._ensure_logger_id()
        self._add_logger_id_filter()

        self.log_format = log_format
        self.date_format = date_format
        self._add_console_log_handler()

        print(f"Logger initialized with ID: {self.logger_id}")

    @abc.abstractmethod
    def log_result(self, payload) -> None:
        """
        Logs a message containing the result of a process as payload,
        using the configured logger.

        :param payload: The payload containing the result to be logged.
        """
        pass

    def _verify_log_level(self) -> None:
        """
        Verifies that the provided log level is valid, defaults to logging.INFO if not.
        """
        if self.log_level not in (
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL,
        ):
            self.log_level = logging.INFO

    def _add_console_log_handler(self) -> None:
        """
        Configures the logger to output logs to the console with specified formatting.
        """
        console_handler_exists = any(
            isinstance(handler, logging.StreamHandler) and handler.stream is sys.stdout
            for handler in self.handlers
        )

        if console_handler_exists:
            print("Console log handler was already added")
        else:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                logging.Formatter(fmt=self.log_format, datefmt=self.date_format)
            )
            self.addHandler(handler)

    def _ensure_logger_id(self):
        if self.logger_id is None or self.logger_id == "":
            self.logger_id = str(uuid.uuid4())

    def _add_logger_id_filter(self):
        """
        Adds a logging filter that includes the logger_id in each log record.
        """

        def filter(record):
            record.logger_id = self.logger_id
            return True

        self.addFilter(filter)
