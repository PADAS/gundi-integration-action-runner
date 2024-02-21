from gundi_core.events import (
    IntegrationActionCustomLog,
    CustomActivityLog,
    LogLevel,
    IntegrationActionFailed,
    ActionExecutionFailed,
    IntegrationActionComplete,
    ActionExecutionComplete,
    IntegrationActionStarted,
    ActionExecutionStarted
)


class ActivityLoggerStoryboard(object):
    @staticmethod
    def given_action_started_event():
        return IntegrationActionStarted(
            payload=ActionExecutionStarted(
                integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
                action_id="pull_observations",
                config_data={
                    "end_datetime": "2024-01-10T00:00:00-00:00",
                    "start_datetime": "2024-01-01T23:59:59-00:00",
                    "force_run_since_start": True
                },
            )
        )

    @staticmethod
    def given_action_complete_event():
        return IntegrationActionComplete(
            payload=ActionExecutionComplete(
                integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
                action_id="pull_observations",
                config_data={
                    "end_datetime": "2024-01-10T00:00:00-00:00",
                    "start_datetime": "2024-01-01T23:59:59-00:00",
                    "force_run_since_start": True
                },
                result={"observations_extracted": 10}
            )
        )

    @staticmethod
    def given_action_failed_event():
        return IntegrationActionFailed(
            payload=ActionExecutionFailed(
                integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
                action_id="pull_observations",
                config_data={
                    "end_datetime": "2024-01-10T00:00:00-00:00",
                    "start_datetime": "2024-01-01T23:59:59-00:00",
                    "force_run_since_start": True
                },
                error="ConnectionError: Error connecting to X system"
            )
        )

    @staticmethod
    def given_custom_activity_log_event():
        return IntegrationActionCustomLog(
            payload=CustomActivityLog(
                integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
                action_id="pull_observations",
                config_data={
                    "end_datetime": "2024-01-01T00:00:00-00:00",
                    "start_datetime": "2024-01-10T23:59:59-00:00",
                    "force_run_since_start": True
                },
                title="Invalid start_datetime for action pull_observations",
                level=LogLevel.ERROR,
                data={
                    "details": "start_datetime cannot be grater than end_datetime. Please fix the configuration."
                }
            )
        )

    def system_event(self, request):
        if request == "action_started_event":
            return self.given_action_started_event()
        if request == "action_complete_event":
            return self.given_action_complete_event()
        if request == "action_failed_event":
            return self.given_action_failed_event()
        if request == "custom_activity_log_event":
            return self.given_custom_activity_log_event()
        return None
