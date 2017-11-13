# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import time
from google.cloud import pubsub_v1
from apps.utils.log import log, error_log


class SubscribeBase(object):

    event = None

    @classmethod
    def received_function(cls, message):
        """
        CallBack内で実行したい処理のAbstract Function
        :param message: Subscribeしたメッセージ
        """
        pass

    @classmethod
    def close(cls):
        """
        event.wait()しているのを処理終了させる
        """
        cls.event.set()

    @classmethod
    def pull(cls, event, subscription_name):
        """
        Pullリクエストを行い、メッセージをSubscribeする。
        callbackメソッドにてメッセージの正常応答と、メッセージに対応した処理を行う。
        :param event: threading.Eventオブジェクト
        :param subscription_name: "projects/"から始まるSubscription名称
        """

        # SubscriberClientを作成
        subscriber = pubsub_v1.SubscriberClient()

        # MessageをPull出来たら実行されるCallBack
        def callback(message):
            log('Received message: {}'.format(message))
            message.ack()
            # メッセージ受信後処理をCall
            cls.received_function(message)

        try:
            # Publisherのメッセージを待ち受ける
            subscriber.subscribe(subscription_name, callback=callback)

            log('Listening for messages on {}'.format(subscription_name))

            # プロセスを常駐させる
            cls.event = event
            cls.event.wait()

            # event.set()されたらログを出して終了
            log('Closed for messages on {}'.format(subscription_name))

        except KeyboardInterrupt:
            log('Stopped Subscribe on {}'.format(subscription_name))
            cls.event.set()
            raise
        except Exception as e:
            error_log('subscription error. detail = {}'.format(e))
            cls.event.set()
            raise
