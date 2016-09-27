# coding=utf-8
from __future__ import unicode_literals
from __future__ import absolute_import
import sys
import argparse
from simple_concurrency_with_amqp.settings import BaseSettings, SettingsConfig
from simple_concurrency_with_amqp.master import Master


class Application(object):

    def run(self):
        parser = argparse.ArgumentParser(description='spawn workers and run concurrency')
        sts = BaseSettings.settings
        for st in sts:
            args, kwargs = sts[st].as_options()
            parser.add_argument(*args, **kwargs)
        args = parser.parse_args()
        if not getattr(args, 'settings', None):
            settings_config = SettingsConfig(args=args)
        else:
            settings_config = SettingsConfig(user_settings_string=args.settings)
        Master(settings_config).run()
