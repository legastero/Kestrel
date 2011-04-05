import os
import sleekxmpp
from sleekxmpp.xmlstream import ElementBase, ET, register_stanza_plugin


class Config(ElementBase):

    name = 'config'
    namespace = 'kestrel:config'


class XMPPConfig(ElementBase):

    name = 'xmpp'
    namespace = 'kestrel:config'
    plugin_attrib = name
    interfaces = set(('jid', 'password', 'server', 'port'))
    sub_interfaces = interfaces


class WorkerConfig(ElementBase):

    name = 'worker'
    namespace = 'kestrel:config'
    plugin_attrib = name
    interfaces = set(('manager', 'features'))
    sub_interfaces = interfaces

    def get_features(self):
        features = set()
        items = self.findall('{%s}feature' % self.namespace)
        if items is not None:
            for item in items:
                features.add(item.text)
        return features


class ManagerConfig(ElementBase):

    name = 'manager'
    namespace = 'kestrel:config'
    plugin_attrib = name
    interfaces = set(('pool', 'jobs'))
    sub_interfaces = interfaces


class ClientConfig(ElementBase):

    name = 'client'
    namespace = 'kestrel:config'
    plugin_attrib = name
    interfaces = set(('pool','submit'))
    sub_interfaces = interfaces


class RedisConfig(ElementBase):

    name = 'redis'
    namespace = 'kestrel:config'
    plugin_attrib = name
    interfaces = set(('host', 'port', 'database'))
    sub_interfaces = interfaces

    def get_port(self):
        port = self._get_sub_text('port')
        if port:
            return int(port)
        return 6973

register_stanza_plugin(Config, WorkerConfig)
register_stanza_plugin(Config, ManagerConfig)
register_stanza_plugin(Config, ClientConfig)
register_stanza_plugin(WorkerConfig, XMPPConfig)
register_stanza_plugin(ManagerConfig, XMPPConfig)
register_stanza_plugin(ClientConfig, XMPPConfig)
register_stanza_plugin(ManagerConfig, RedisConfig)


def load_config(file_name):
    file_name = os.path.expanduser(file_name)
    with open(file_name, 'r+') as file:
        data = "\n".join([line for line in file])
        config = Config(xml=ET.fromstring(data))
        return config
