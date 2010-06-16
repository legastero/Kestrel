# Kestrel: An XMPP-based Job Scheduler
# Author: Lance Stout <lancestout@gmail.com>
#
# Credits: Nathan Fritz <fritzy@netflint.net>
#
# Copyright 2010 Lance Stout
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sleekxmpp
from sleekxmpp.xmlstream.stanzabase import ElementBase, ET, JID


class Job(ElementBase):
    namespace = 'kestrel:job'
    name = 'job'
    plugin_attrib = 'kestrel_job'
    interfaces = set(('id', 'action', 'command', 'cleanup', 'queue', 'status', 'requirements'))
    sub_interfaces = set(('command', 'cleanup'))

    def getQueue(self):
        return int(self.xml.attrib.get('queue', 1))

    def getRequirements(self):
        requirements = set()
        reqs = self.xml.findall('{%s}requires' % self.namespace)
        for req in reqs:
            requirements.add(req.text)

        reqs = list(requirements)
        reqs.sort()
        return reqs

    def setRequirements(self, requirements):
        for req in requirements:
            self.addRequirement(req)

    def delRequirements(self):
        reqsXML = self.xml.findall('{%s}requires' % self.namespace)
        for reqXML in reqsXML:
            self.xml.remove(reqXML)

    def addRequirement(self, req):
        reqXML = ET.Element('{%s}requires' % self.namespace)
        reqXML.text = req
        self.xml.append(reqXML)

    def delRequirement(self, req):
        reqsXML = self.xml.findall('{%s}requires' % self.namespace)
        for reqXML in reqsXML:
            if reqXML.text == reg:
                self.xml.remove(reqXML)
