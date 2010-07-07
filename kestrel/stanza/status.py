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


class JobStatus(ElementBase):
    namespace = 'kestrel:status'
    name = 'job'
    plugin_attrib = 'job'
    interfaces = set(('id', 'owner', 'requested', 'queued', 'running', 'completed'))
    sub_interfaces = set(('requested', 'queued', 'running', 'completed'))


class PoolStatus(ElementBase):
    namespace = 'kestrel:status'
    name = 'pool'
    plugin_attrib = 'pool'
    interfaces = set(('available', 'busy', 'online'))
    sub_interfaces = set(('available', 'busy', 'online'))


class Status(ElementBase):
    namespace = 'kestrel:status'
    name = 'query'
    plugin_attrib = 'kestrel_status'
    interfaces = set(('id', 'jobs'))
    subitem = (PoolStatus, JobStatus)

    def addJob(self, job_id, owner, requested, queued, running, completed):
        job = JobStatus(parent=self)
        job['id'] = str(job_id)
        job['owner'] = str(owner)
        job['requested'] = str(requested)
        job['queued'] = str(queued)
        job['running'] = str(running)
        job['completed'] = str(completed)

    def delJob(self, job_id):
        jobsXML = self.xml.findall('{%s}job' % JobStatus.namespace)
        for jobXML in jobsXML:
            if jobXML.attrib.get('id', '') == job_id:
                self.xml.remove(jobXML)
                return

    def getJobs(self):
        jobs = []
        jobsXML = self.xml.findall('{%s}job' % JobStatus.namespace)
        for jobXML in jobsXML:
            jobs.append(JobStatus(parent=None, xml=jobXML))
        return jobs

    def setJobs(self, jobs):
        self.delJobs()
        for job in jobs:
            self.addJob(job['id'], job['queued'], job['running'], job['completed'])

    def delJobs(self):
        jobsXML = self.xml.findall('{%s}job' % JobStatus.namespace)
        for jobXML in jobsXML:
            self.xml.remove(jobXML)
