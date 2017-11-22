# -*- coding: utf-8 -*-
from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals
)

from google.cloud import pubsub_v1
from apps.utils.log import log
from settings import GCP_PROJECT_ID


def _encode_byteString(target):
    if isinstance(target, unicode):
        return target.encode('utf-8')
    else:
        return target


def publish(topic_name, message_data, **kwargs):
    """受け取った値を元にPub/SubメッセージをPublishする

    :param topic_name: Publish target Topic 
    :param message_data: Publish message data
    :param kwargs: (optional)Publish message attr
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, topic_name)
    log('Publishing for message on {}'.format(topic_name))

    data = _encode_byteString(message_data)
    publisher.publish(topic_path, data=data, **kwargs)

    log('Published message.')
