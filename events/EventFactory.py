from events.AwardPointEvent import AwardPointEvent
from events.EventNotificationAdmin import EventNotificationAdmin
from events.EventNotificationAdminResend import EventNotificationAdminResend
from events.FollowUserEvent import FollowUserEvent
from events.RejectReviewEvent import RejectReviewEvents

from .CreatePostEvent import CreatePostEvents
from .ReactPostEvent import ReactPostEvents
from .SaleEvents import SaleEvents
from .ReplyPostEvent import ReplyPostEvents
from .UserRequestEvent import UserRequestEvent


class EventFactory():
    factories = [
        CreatePostEvents, 
        ReactPostEvents,
        SaleEvents,
        ReplyPostEvents,
        EventNotificationAdmin,
        EventNotificationAdminResend,
        FollowUserEvent,
        AwardPointEvent,
        UserRequestEvent,
        RejectReviewEvents,
    ]

    def getEvent(self, type):
        for fact in self.factories:
            if type == fact.name:
                return fact()
        return None