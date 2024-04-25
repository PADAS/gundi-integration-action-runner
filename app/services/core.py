from enum import Enum


class ActionTypeEnum(str, Enum):
    AUTHENTICATION = "auth"
    PULL_DATA = "pull"
    PUSH_DATA = "push"
    GENERIC = "generic"
