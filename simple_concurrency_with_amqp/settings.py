# coding=utf-8
from __future__ import unicode_literals
from __future__ import absolute_import
import importlib
import copy
import sys


class SettingsException(Exception):
    pass


class BaseSettings(object):
    settings = {}

    @classmethod
    def register(cls, field):
        cls.settings[field.name] = field()


def reload_settings_module(self):
    if not self.value:
        return
    if self.value not in sys.modules:
        sys.modules[self.value] = importlib.import_module(self.value)
    reload(sys.modules[self.value])


class SettingsField(object):
    required = False
    name = None
    value = None
    arg = None
    type = None
    help = ''

    def reload(self):
        raise NotImplemented

    def load(self):
        raise NotImplemented

    def copy(self):
        return copy.copy(self)

    def __init__(self):
        if self.name is None:
            raise SettingsException('%s must have a name' % self.__class__.__name__)

    def as_options(self):
        args = [self.arg]
        kwargs = dict(dest=self.name)
        if self.value:
            kwargs['default'] = self.value
        if self.help:
            kwargs['help'] = self.help
        if self.type:
            kwargs['type'] = self.type
        return args, kwargs


@BaseSettings.register
class WorkerTimeout(SettingsField):
    required = False
    name = 'worker_timeout'
    value = 300
    type = int
    arg = '--worker-timeout'
    help = 'worker timeout'

    def load(self):
        pass

    def reload(self):
        pass


@BaseSettings.register
class TaskModuleString(SettingsField):
    required = True
    value = None
    name = 'task_module_str'
    arg = '--task-module-str'
    help = 'path.to.your.task.module'

    def load(self):
        pass

    def reload(self):
        reload_settings_module(self)


@BaseSettings.register
class Workers(SettingsField):
    required = False
    type = int
    name = 'workers'
    value = 1
    arg = '--workers'
    help = 'worker number'

    def load(self):
        pass

    def reload(self):
        pass


@BaseSettings.register
class Settings(SettingsField):
    required = False
    name = 'settings'
    arg = '--settings'
    help = 'your.settings.path'

    def load(self):
        pass

    def reload(self):
        reload_settings_module(self)


class SettingsConfig(object):

    def __init__(self, args=None, user_settings_string=None):
        self.settings = {}
        self.args = args
        self.user_settings_string = user_settings_string
        self.compact_settings()

    def __getattr__(self, key):
        return self.settings[key].value

    @property
    def task_module(self):
        task_module_str = self.settings['task_module_str'].value
        if task_module_str in sys.modules:
            return sys.modules[task_module_str]
        return importlib.import_module(task_module_str)

    def compact_settings(self):
        if self.user_settings_string:
            if self.user_settings_string in sys.modules:
                reload(sys.modules[self.user_settings_string])
                self.user_settings = sys.modules[self.user_settings_string]
            else:
                self.user_settings = importlib.import_module(self.user_settings_string)
        else:
            self.user_settings = self.args
        for st in BaseSettings.settings:
            self.settings[st] = BaseSettings.settings[st].copy()
            if getattr(self.user_settings, st, None) is not None:
                self.settings[st].value = getattr(self.user_settings, st)
            elif self.settings[st].required is True and self.settings[st].value is None:
                raise SettingsException('%s must be set a value' % BaseSettings.settings[st].name)

    def reload(self):
        self.compact_settings()
        for st in self.settings:
            self.settings[st].reload()
