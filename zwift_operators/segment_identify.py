from typing import Dict, Optional

from airflow.models import BaseOperator
from airflow.providers.segment.hooks.segment import SegmentHook
from airflow.utils.decorators import apply_defaults


class SegmentTrackEventOperator(BaseOperator):
    """
    Send Track Event to Segment for a specified user_id and event

    :param user_id: The ID for this user in your database. (templated)
    :type user_id: str
    :param event: The name of the event you're tracking. (templated)
    :type event: str
    :param properties: A dictionary of properties for the event. (templated)
    :type properties: dict
    :param segment_conn_id: The connection ID to use when connecting to Segment.
    :type segment_conn_id: str
    :param segment_debug_mode: Determines whether Segment should run in debug mode.
        Defaults to False
    :type segment_debug_mode: bool
    """

    template_fields = ('user_id', 'event', 'properties')
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(
            self,
            *,
            user_id: str,
            event: str,
            properties: Optional[dict] = None,
            segment_conn_id: str = 'segment_default',
            segment_debug_mode: bool = False,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.user_id = user_id
        self.event = event
        properties = properties or {}
        self.properties = properties
        self.segment_debug_mode = segment_debug_mode
        self.segment_conn_id = segment_conn_id

    def execute(self, context: Dict) -> None:
        hook = SegmentHook(segment_conn_id=self.segment_conn_id,
                           segment_debug_mode=self.segment_debug_mode)

        self.log.info(
            'Sending track event (%s) for user id: %s with properties: %s',
            self.event,
            self.user_id,
            self.properties,
        )

        # pylint: disable=no-member
        hook.identify(
            user_id=self.user_id, properties=self.properties
        )
